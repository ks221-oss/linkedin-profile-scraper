import os
import csv
import json
import uuid
import threading
import time
from datetime import datetime, timezone
from flask import Flask, request, jsonify, send_file, render_template
from dotenv import load_dotenv
import requests as http_requests

load_dotenv()

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["OUTPUT_FOLDER"] = "outputs"
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10MB

# Ensure directories exist at import time (also for gunicorn workers)
os.makedirs("uploads", exist_ok=True)
os.makedirs("outputs", exist_ok=True)

APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")

# Primary actor (dev_fusion)
ACTOR_ID_PRIMARY = "dev_fusion~Linkedin-Profile-Scraper"
ACTOR_PAYLOAD_PRIMARY = lambda urls: {"profileUrls": urls}

# Fallback actor (harvestapi/linkedin-profile-scraper)
# Uses 'queries' field (not 'targetUrls' - that's the UI label) and requires profileScraperMode
ACTOR_ID_FALLBACK = "LpVuK3Zozwuipa5bp"
ACTOR_PAYLOAD_FALLBACK = lambda urls: {
    "queries": urls,
    "profileScraperMode": "Profile details no email ($4 per 1k)"
}

BASE_URL = "https://api.apify.com/v2"
BATCH_SIZE = 10


def normalize_linkedin_url(url: str) -> str:
    """Strip query params and fragments from a LinkedIn URL.
    e.g. https://www.linkedin.com/in/user/?originalSubdomain=in  →  https://www.linkedin.com/in/user/
    """
    url = url.strip()
    for char in ("?", "#"):
        if char in url:
            url = url[:url.index(char)]
    if not url.endswith("/"):
        url += "/"
    return url

# Persistent job tracker: job_id -> { status, progress, total, output_file, error, ... }
JOBS_FILE = "jobs.json"
jobs = {}
jobs_lock = threading.Lock()


def load_jobs():
    """Load jobs from disk. Mark interrupted jobs as errored on startup."""
    global jobs
    if not os.path.exists(JOBS_FILE):
        jobs = {}
        return

    try:
        with open(JOBS_FILE, "r") as f:
            jobs = json.load(f)

        # Any job that was running when the server restarted is now interrupted
        interrupted_count = 0
        for job_id, job in jobs.items():
            if job.get("status") in ("queued", "reading", "scraping"):
                job["status"] = "error"
                job["error"] = "Run was interrupted (server restarted). Please upload the file again to retry."
                interrupted_count += 1

        if interrupted_count > 0:
            print(f"⚠️  Marked {interrupted_count} interrupted job(s) as errored on startup")
            save_jobs()
    except Exception as e:
        print(f"Could not load jobs.json: {e}")
        jobs = {}


def save_jobs():
    """Persist jobs dict to disk. Thread-safe."""
    with jobs_lock:
        try:
            tmp = JOBS_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(jobs, f, indent=2, default=str)
            os.replace(tmp, JOBS_FILE)
        except Exception as e:
            print(f"Could not save jobs.json: {e}")


def update_job(job_id: str, **fields):
    """Update a job's fields and persist to disk."""
    if job_id in jobs:
        jobs[job_id].update(fields)
        save_jobs()


# Load jobs at module import time (so gunicorn workers also load history)
load_jobs()


# ── Scraping Logic ────────────────────────────────────────

# Retry config for handling transient API errors (502, 503, 504, connection errors)
MAX_RETRIES = 5
RETRY_BACKOFF_SEC = [2, 5, 10, 20, 30]  # Exponential backoff
TRANSIENT_STATUS_CODES = (500, 502, 503, 504, 408, 429)


def apify_request(method: str, url: str, **kwargs):
    """Make an Apify API request with retry logic for transient failures.
    Retries on 5xx errors, 429 rate limits, 408 timeouts, and connection errors.
    Returns the response object if successful, raises the last exception if all retries fail.
    """
    last_exception = None

    for attempt in range(MAX_RETRIES):
        try:
            resp = http_requests.request(method, url, timeout=30, **kwargs)

            # If transient error, retry
            if resp.status_code in TRANSIENT_STATUS_CODES:
                wait = RETRY_BACKOFF_SEC[min(attempt, len(RETRY_BACKOFF_SEC) - 1)]
                print(f"  ⚠️  Apify API returned {resp.status_code}, retrying in {wait}s "
                      f"(attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            # Success or non-retryable error - return immediately
            return resp

        except (http_requests.exceptions.ConnectionError,
                http_requests.exceptions.Timeout,
                http_requests.exceptions.ChunkedEncodingError) as e:
            last_exception = e
            wait = RETRY_BACKOFF_SEC[min(attempt, len(RETRY_BACKOFF_SEC) - 1)]
            print(f"  ⚠️  Connection error: {type(e).__name__}, retrying in {wait}s "
                  f"(attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait)

    # All retries exhausted
    if last_exception:
        raise last_exception
    # If we got transient HTTP errors all the way through, return the last response
    return resp


def scrape_batch_with_actor(urls: list[str], actor_id: str, payload_builder) -> list[dict]:
    """Run a single Apify actor and return results."""
    params = {"token": APIFY_TOKEN}
    run_url = f"{BASE_URL}/acts/{actor_id}/runs"
    payload = payload_builder(urls)

    try:
        resp = apify_request("POST", run_url, json=payload,
                             headers={"Content-Type": "application/json"}, params=params)
        resp.raise_for_status()
    except http_requests.exceptions.HTTPError as e:
        if e.response.status_code == 402:
            print(f"  ⚠️  Actor {actor_id}: Out of credits (402)")
        else:
            print(f"  ⚠️  Actor {actor_id}: HTTP {e.response.status_code}")
        return []
    except Exception as e:
        print(f"  ⚠️  Actor {actor_id}: Failed to start run - {type(e).__name__}: {e}")
        return []

    run_id = resp.json()["data"]["id"]
    print(f"  Started run: {run_id}")

    status_url = f"{BASE_URL}/actor-runs/{run_id}"
    poll_count = 0
    while True:
        try:
            resp = apify_request("GET", status_url, params=params)
            resp.raise_for_status()
        except Exception as e:
            # If polling fails after retries, log and try again on next iteration
            print(f"  ⚠️  Polling error (will retry): {type(e).__name__}: {e}")
            time.sleep(5)
            poll_count += 1
            if poll_count > 60:  # Give up after ~3+ minutes of polling failures
                print(f"  ❌ Gave up polling after {poll_count} failed attempts")
                return []
            continue

        run_info = resp.json()["data"]
        status = run_info["status"]
        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"  ⚠️  Actor {actor_id}: Run {status}")
            return []
        time.sleep(3)

    dataset_id = run_info["defaultDatasetId"]
    items_url = f"{BASE_URL}/datasets/{dataset_id}/items"
    try:
        resp = apify_request("GET", items_url, params=params)
        resp.raise_for_status()
        results = resp.json()
        return results if results else []
    except Exception as e:
        print(f"  ⚠️  Failed to fetch results: {type(e).__name__}: {e}")
        return []


def scrape_batch(urls: list[str]) -> list[dict]:
    """Try primary actor, fallback to secondary if needed.
    Tags each profile with `_source_actor` so we know which actor produced the data.
    """
    print(f"  Trying primary actor: {ACTOR_ID_PRIMARY}")
    results = scrape_batch_with_actor(urls, ACTOR_ID_PRIMARY, ACTOR_PAYLOAD_PRIMARY)

    # Check if primary actor returned valid data (not just empty results)
    has_valid_data = results and any(r.get("fullName") or r.get("firstName") for r in results)

    if has_valid_data:
        print(f"  ✅ Primary actor succeeded with {len(results)} profiles")
        for r in results:
            r["_source_actor"] = "primary"
        return results

    print(f"  ⚠️  Primary actor returned no valid data. Trying fallback...")
    print(f"  Trying fallback actor: {ACTOR_ID_FALLBACK}")
    results = scrape_batch_with_actor(urls, ACTOR_ID_FALLBACK, ACTOR_PAYLOAD_FALLBACK)

    if results and any(r.get("fullName") or r.get("firstName") for r in results):
        print(f"  ✅ Fallback actor succeeded with {len(results)} profiles")
        for r in results:
            r["_source_actor"] = "fallback"
    else:
        print(f"  ❌ Both actors failed")

    return results


# ── Normalization helpers ──────────────────────────────────

MONTH_TO_NUM = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def _format_date_short(date_obj):
    """Convert a fallback-actor date object to 'M-YYYY' string.
    Examples:
      {"month": "Nov", "year": 2025, "text": "Nov 2025"} → "11-2025"
      {"text": "Present"} → "Present"
      {"month": null, "year": 2019, "text": "2019"} → "2019"
    """
    if not isinstance(date_obj, dict):
        return ""
    text = date_obj.get("text", "")
    if text == "Present":
        return "Present"
    month = date_obj.get("month")
    year = date_obj.get("year")
    if month and year:
        m_num = MONTH_TO_NUM.get(month, "")
        return f"{m_num}-{year}" if m_num else str(year)
    if year:
        return str(year)
    return text or ""


def _format_year(date_obj):
    """Extract just the year from a date dict."""
    if not isinstance(date_obj, dict):
        return ""
    year = date_obj.get("year")
    if year:
        return str(year)
    return date_obj.get("text", "") or ""


def format_experiences(profile: dict) -> str:
    """Format the experiences/experience field as a human-readable string.
    Output: 'Title @ Company (M-YYYY - M-YYYY) | Title @ Company (M-YYYY - Present)'
    Handles both primary (dev_fusion) and fallback (harvestapi) formats.
    """
    # Primary actor uses 'experiences', fallback uses 'experience'
    experiences = profile.get("experiences") or profile.get("experience") or []
    if not isinstance(experiences, list):
        return ""

    lines = []
    for job in experiences:
        if not isinstance(job, dict):
            continue

        # Primary: title; Fallback: position
        title = job.get("title") or job.get("position") or ""
        company = job.get("companyName") or job.get("company") or ""

        # Primary: jobStartedOn is already 'M-YYYY' string
        # Fallback: startDate is a dict
        if isinstance(job.get("startDate"), dict):
            started = _format_date_short(job["startDate"])
            ended = _format_date_short(job.get("endDate")) if job.get("endDate") else ""
        else:
            started = job.get("jobStartedOn", "") or ""
            ended = job.get("jobEndedOn") or ("Present" if job.get("jobStillWorking") else "")

        period = f" ({started} - {ended})" if started else ""
        if title or company:
            lines.append(f"{title} @ {company}{period}".strip())

    return " | ".join(lines)


def format_education(profile: dict) -> str:
    """Format the education/educations field as a human-readable string.
    Output: 'School — Degree, FieldOfStudy (YYYY-YYYY) | ...'
    """
    # Primary actor uses 'educations', fallback uses 'education'
    educations = profile.get("educations") or profile.get("education") or []
    if not isinstance(educations, list):
        return ""

    lines = []
    for edu in educations:
        if not isinstance(edu, dict):
            continue

        # Primary: title (school) + subtitle (degree)
        # Fallback: schoolName + degree + fieldOfStudy
        school = edu.get("schoolName") or edu.get("title") or ""
        degree = edu.get("degree") or edu.get("subtitle") or ""
        field = edu.get("fieldOfStudy", "") or ""

        # Combine degree + field of study for fallback actor
        if degree and field:
            degree_str = f"{degree}, {field}"
        else:
            degree_str = degree or field

        # Year range
        if isinstance(edu.get("startDate"), dict) or isinstance(edu.get("endDate"), dict):
            # Fallback format
            start_year = _format_year(edu.get("startDate"))
            end_year = _format_year(edu.get("endDate"))
        else:
            # Primary format
            period = edu.get("period", {}) if isinstance(edu.get("period"), dict) else {}
            start_year = (period.get("startedOn", {}) or {}).get("year", "") if isinstance(period.get("startedOn"), dict) else ""
            end_year = (period.get("endedOn", {}) or {}).get("year", "") if isinstance(period.get("endedOn"), dict) else ""

        year_range = f" ({start_year}-{end_year})" if start_year or end_year else ""

        if school and degree_str:
            lines.append(f"{school} — {degree_str}{year_range}")
        elif school:
            lines.append(f"{school}{year_range}")

    return " | ".join(lines)


def format_skills(profile: dict) -> str:
    """Format skills as a comma-separated list of skill names.
    Output: 'Skill 1, Skill 2, Skill 3'
    """
    skills = profile.get("skills") or []
    if not isinstance(skills, list):
        return ""

    names = []
    for s in skills:
        if isinstance(s, dict):
            # Primary uses 'title', fallback uses 'name'
            name = s.get("name") or s.get("title") or ""
            if name:
                names.append(name)
        elif isinstance(s, str):
            names.append(s)

    return ", ".join(names)


def flatten_profile(profile: dict) -> dict:
    """Normalize profile data from EITHER actor into a fixed set of columns.
    Maps fallback actor fields to the primary actor's column structure.
    """
    if not profile or "error" in profile:
        return {k: "" for k in get_scraped_columns()}

    is_fallback = profile.get("_source_actor") == "fallback"

    # Name fields (work the same in both actors)
    first_name = profile.get("firstName", "") or ""
    last_name = profile.get("lastName", "") or ""
    full_name = profile.get("fullName") or f"{first_name} {last_name}".strip()

    if is_fallback:
        # Fallback (harvestapi) field mapping
        current_position = profile.get("currentPosition") or {}
        if isinstance(current_position, list) and current_position:
            current_position = current_position[0]
        if not isinstance(current_position, dict):
            current_position = {}

        location_obj = profile.get("location") or {}
        if not isinstance(location_obj, dict):
            location_obj = {}

        return {
            "full_name": full_name,
            "first_name": first_name,
            "last_name": last_name,
            "headline": profile.get("headline", "") or "",
            "current_job_title": current_position.get("position", "") or "",
            "current_company": current_position.get("companyName", "") or "",
            "current_company_industry": "",
            "current_company_size": "",
            "current_company_website": "",
            "current_job_location": "",
            "current_job_duration": "",
            "location": location_obj.get("linkedinText", "") or "",
            "country": location_obj.get("countryCode", "") or "",
            "about": profile.get("about", "") or "",
            "email": "",
            "phone": "",
            "connections": profile.get("connectionsCount", "") or "",
            "followers": profile.get("followerCount", "") or "",
            "total_experience_years": "",
            "is_premium": "true" if profile.get("premium") is True else "false" if profile.get("premium") is False else "",
            "is_verified": "",
            "is_creator": "",
            "experiences": format_experiences(profile),
            "education": format_education(profile),
            "skills": format_skills(profile),
        }
    else:
        # Primary (dev_fusion) field mapping
        return {
            "full_name": full_name,
            "first_name": first_name,
            "last_name": last_name,
            "headline": profile.get("headline", "") or "",
            "current_job_title": profile.get("jobTitle", "") or "",
            "current_company": profile.get("companyName", "") or "",
            "current_company_industry": profile.get("companyIndustry", "") or "",
            "current_company_size": profile.get("companySize", "") or "",
            "current_company_website": profile.get("companyWebsite", "") or "",
            "current_job_location": profile.get("jobLocation", "") or "",
            "current_job_duration": profile.get("currentJobDuration", "") or "",
            "location": profile.get("addressWithCountry", "") or "",
            "country": profile.get("addressCountryOnly", "") or "",
            "about": profile.get("about", "") or "",
            "email": profile.get("email", "") or "",
            "phone": profile.get("mobileNumber", "") or "",
            "connections": profile.get("connections", "") or "",
            "followers": profile.get("followers", "") or "",
            "total_experience_years": profile.get("totalExperienceYears", "") or "",
            "is_premium": "true" if profile.get("isPremium") is True else "false" if profile.get("isPremium") is False else "",
            "is_verified": "true" if profile.get("isVerified") is True else "false" if profile.get("isVerified") is False else "",
            "is_creator": "true" if profile.get("isCreator") is True else "false" if profile.get("isCreator") is False else "",
            "experiences": format_experiences(profile),
            "education": format_education(profile),
            "skills": format_skills(profile),
        }


def get_scraped_columns():
    """Fixed column order for the output CSV."""
    return [
        "full_name", "first_name", "last_name", "headline",
        "current_job_title", "current_company",
        "current_company_industry", "current_company_size",
        "current_company_website", "current_job_location",
        "current_job_duration",
        "location", "country", "about", "email", "phone",
        "connections", "followers", "total_experience_years",
        "is_premium", "is_verified", "is_creator",
        "experiences", "education", "skills",
    ]


def find_url_column(headers):
    for h in headers:
        if any(k in h.lower() for k in ["linkedin", "url", "link", "profile"]):
            return h
    return headers[0]


def process_job(job_id: str, input_path: str, output_path: str):
    try:
        update_job(job_id, status="reading")

        with open(input_path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            input_headers = reader.fieldnames or []
            rows = list(reader)

        url_col = find_url_column(input_headers)
        raw_urls = [(row.get(url_col) or "").strip() for row in rows]
        urls = [normalize_linkedin_url(u) if u and "linkedin.com/in/" in u else u for u in raw_urls]
        valid_urls = [u for u in urls if u and "linkedin.com/in/" in u]

        # Log normalized URLs for debugging
        for raw, clean in zip(raw_urls, urls):
            if raw != clean:
                print(f"  [URL normalized] {raw}  →  {clean}")
            else:
                print(f"  [URL clean]      {clean}")

        update_job(job_id, total=len(valid_urls), status="scraping")

        # Scrape in batches
        all_profiles = {}
        scraped = 0
        for i in range(0, len(valid_urls), BATCH_SIZE):
            batch = valid_urls[i:i + BATCH_SIZE]
            results = scrape_batch(batch)
            for profile in results:
                profile_url = profile.get("linkedinUrl", "")
                pub_id = profile.get("publicIdentifier", "")
                if profile_url:
                    all_profiles[profile_url] = profile
                if pub_id:
                    all_profiles[pub_id] = profile
            scraped += len(batch)
            update_job(job_id, progress=scraped)

        # ── Build output with normalized columns ───────────────────
        scraped_cols = get_scraped_columns()
        output_headers = list(input_headers) + [f"scraped_{c}" for c in scraped_cols]
        # Always add a source-actor column for transparency
        output_headers.append("scraped__source_actor")

        matched = 0
        failed_urls = []
        output_rows = []
        for row, url in zip(rows, urls):
            out_row = dict(row)
            flat = {c: "" for c in scraped_cols}
            source_actor = ""

            # Replace the original URL column with the cleaned URL
            if url and "linkedin.com/in/" in url:
                out_row[url_col] = url

                profile = all_profiles.get(url)
                if not profile:
                    pub_id = url.rstrip("/").split("/in/")[-1]
                    profile = all_profiles.get(pub_id)
                if profile and "error" not in profile:
                    flat = flatten_profile(profile)
                    source_actor = profile.get("_source_actor", "primary")
                    matched += 1
                else:
                    failed_urls.append(url)
                    print(f"  [no data] {url}")

            for col in scraped_cols:
                out_row[f"scraped_{col}"] = flat.get(col, "")
            out_row["scraped__source_actor"] = source_actor
            output_rows.append(out_row)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=output_headers, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(output_rows)

        update_job(
            job_id,
            status="done",
            matched=matched,
            failed_count=len(failed_urls),
            failed_urls=failed_urls,
            output_file=output_path,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )

    except Exception as e:
        # Provide user-friendly error messages for common issues
        err_str = str(e)
        if "502" in err_str or "Bad Gateway" in err_str:
            err_msg = "Apify's servers are temporarily unavailable (502). Please try again in a minute."
        elif "503" in err_str or "Service Unavailable" in err_str:
            err_msg = "Apify service is temporarily down (503). Please try again shortly."
        elif "504" in err_str or "Gateway Timeout" in err_str:
            err_msg = "Apify request timed out (504). Please try again with a smaller batch."
        elif "402" in err_str or "Payment Required" in err_str:
            err_msg = "Apify account is out of credits. Please check your billing."
        elif "401" in err_str or "Unauthorized" in err_str:
            err_msg = "Apify API token is invalid or expired."
        elif "Connection" in err_str or "Timeout" in err_str:
            err_msg = "Network connection issue. Please check your internet and try again."
        else:
            err_msg = f"Unexpected error: {err_str}"

        update_job(
            job_id,
            status="error",
            error=err_msg,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        print(f"  ❌ Job {job_id} failed: {err_str}")


# ── Routes ────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    if not APIFY_TOKEN:
        return jsonify({"error": "APIFY_API_TOKEN is not configured. Please add it to your environment secrets and restart the app."}), 500

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if not file.filename.endswith(".csv"):
        return jsonify({"error": "Please upload a CSV file"}), 400

    job_id = str(uuid.uuid4())
    input_path = os.path.join(app.config["UPLOAD_FOLDER"], f"{job_id}_input.csv")
    output_path = os.path.join(app.config["OUTPUT_FOLDER"], f"{job_id}_enriched.csv")

    file.save(input_path)

    jobs[job_id] = {
        "job_id": job_id,
        "filename": file.filename,
        "status": "queued",
        "progress": 0,
        "total": 0,
        "matched": 0,
        "failed_count": 0,
        "input_file": input_path,
        "output_file": None,
        "error": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
    }
    save_jobs()

    thread = threading.Thread(target=process_job, args=(job_id, input_path, output_path))
    thread.daemon = True
    thread.start()

    return jsonify({"job_id": job_id})


@app.route("/status/<job_id>")
def status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)


@app.route("/history")
def history():
    """Return all jobs sorted by creation time (newest first)."""
    # Strip out heavy fields like failed_urls list for the list view
    summary = []
    for job_id, job in jobs.items():
        summary.append({
            "job_id": job_id,
            "filename": job.get("filename", "untitled.csv"),
            "status": job.get("status"),
            "progress": job.get("progress", 0),
            "total": job.get("total", 0),
            "matched": job.get("matched", 0),
            "failed_count": job.get("failed_count", 0),
            "error": job.get("error"),
            "created_at": job.get("created_at"),
            "completed_at": job.get("completed_at"),
            "has_input": bool(job.get("input_file") and os.path.exists(job.get("input_file", ""))),
            "has_output": bool(job.get("output_file") and os.path.exists(job.get("output_file", ""))),
        })
    # Newest first
    summary.sort(key=lambda j: j.get("created_at") or "", reverse=True)
    return jsonify(summary)


@app.route("/download/<job_id>")
def download(job_id):
    job = jobs.get(job_id)
    if not job or job.get("status") != "done":
        return jsonify({"error": "File not ready"}), 404
    output_file = job.get("output_file")
    if not output_file or not os.path.exists(output_file):
        return jsonify({"error": "Output file not found"}), 404

    base_name = os.path.splitext(job.get("filename", "linkedin"))[0]
    return send_file(
        output_file,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"{base_name}_enriched.csv",
    )


@app.route("/download-input/<job_id>")
def download_input(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    input_file = job.get("input_file")
    if not input_file or not os.path.exists(input_file):
        return jsonify({"error": "Input file not found"}), 404
    return send_file(
        input_file,
        mimetype="text/csv",
        as_attachment=True,
        download_name=job.get("filename", "input.csv"),
    )


@app.route("/job/<job_id>", methods=["DELETE"])
def delete_job(job_id):
    """Delete a job and its associated files."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    # Don't allow deleting active jobs
    if job.get("status") in ("queued", "reading", "scraping"):
        return jsonify({"error": "Cannot delete an active job"}), 400

    # Remove files
    for path_key in ("input_file", "output_file"):
        path = job.get(path_key)
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except Exception as e:
                print(f"Could not remove {path}: {e}")

    # Remove from jobs dict
    del jobs[job_id]
    save_jobs()
    return jsonify({"success": True})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
