import os
import csv
import uuid
import threading
import time
from flask import Flask, request, jsonify, send_file, render_template
from dotenv import load_dotenv
import requests as http_requests

load_dotenv()

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["OUTPUT_FOLDER"] = "outputs"
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10MB

APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")
ACTOR_ID = "dev_fusion~Linkedin-Profile-Scraper"
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

# In-memory job tracker: job_id -> { status, progress, total, output_file, error }
jobs = {}


# ── Scraping Logic ────────────────────────────────────────

def scrape_batch(urls: list[str]) -> list[dict]:
    params = {"token": APIFY_TOKEN}
    run_url = f"{BASE_URL}/acts/{ACTOR_ID}/runs"
    resp = http_requests.post(run_url, json={"profileUrls": urls},
                              headers={"Content-Type": "application/json"}, params=params)
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]

    status_url = f"{BASE_URL}/actor-runs/{run_id}"
    while True:
        resp = http_requests.get(status_url, params=params)
        resp.raise_for_status()
        run_info = resp.json()["data"]
        status = run_info["status"]
        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            return []
        time.sleep(3)

    dataset_id = run_info["defaultDatasetId"]
    items_url = f"{BASE_URL}/datasets/{dataset_id}/items"
    resp = http_requests.get(items_url, params=params)
    resp.raise_for_status()
    return resp.json()


def flatten_profile(profile: dict) -> dict:
    if not profile or "error" in profile:
        return {k: "" for k in get_scraped_columns()}

    experiences = profile.get("experiences") or []
    exp_lines = []
    for job in experiences:
        title = job.get("title", "")
        company = job.get("companyName", "")
        started = job.get("jobStartedOn", "")
        ended = job.get("jobEndedOn") or ("Present" if job.get("jobStillWorking") else "")
        period = f" ({started} - {ended})" if started else ""
        exp_lines.append(f"{title} @ {company}{period}")

    educations = profile.get("educations") or []
    edu_lines = []
    for edu in educations:
        school = edu.get("title", "")
        degree = edu.get("subtitle", "")
        period = edu.get("period", {})
        start_year = period.get("startedOn", {}).get("year", "")
        end_year = period.get("endedOn", {}).get("year", "")
        yr = f" ({start_year}-{end_year})" if start_year or end_year else ""
        edu_lines.append(f"{school} — {degree}{yr}" if degree else f"{school}{yr}")

    skills = profile.get("skills") or []
    skill_names = [s.get("title", str(s)) if isinstance(s, dict) else str(s) for s in skills]

    return {
        "full_name": profile.get("fullName", ""),
        "first_name": profile.get("firstName", ""),
        "last_name": profile.get("lastName", ""),
        "headline": profile.get("headline", ""),
        "current_job_title": profile.get("jobTitle", ""),
        "current_company": profile.get("companyName", ""),
        "current_company_industry": profile.get("companyIndustry", ""),
        "current_company_size": profile.get("companySize", ""),
        "current_company_website": profile.get("companyWebsite", ""),
        "current_job_location": profile.get("jobLocation", ""),
        "current_job_duration": profile.get("currentJobDuration", ""),
        "location": profile.get("addressWithCountry", ""),
        "country": profile.get("addressCountryOnly", ""),
        "about": profile.get("about", ""),
        "email": profile.get("email", ""),
        "phone": profile.get("mobileNumber", ""),
        "connections": profile.get("connections", ""),
        "followers": profile.get("followers", ""),
        "total_experience_years": profile.get("totalExperienceYears", ""),
        "is_premium": profile.get("isPremium", ""),
        "is_verified": profile.get("isVerified", ""),
        "is_creator": profile.get("isCreator", ""),
        "experiences": " | ".join(exp_lines),
        "education": " | ".join(edu_lines),
        "skills": ", ".join(skill_names),
    }


def get_scraped_columns():
    return [
        "full_name", "first_name", "last_name", "headline",
        "current_job_title", "current_company", "current_company_industry",
        "current_company_size", "current_company_website", "current_job_location",
        "current_job_duration", "location", "country", "about", "email", "phone",
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
        jobs[job_id]["status"] = "reading"

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

        jobs[job_id]["total"] = len(valid_urls)
        jobs[job_id]["status"] = "scraping"

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
            jobs[job_id]["progress"] = scraped

        # Build output
        scraped_cols = get_scraped_columns()
        output_headers = list(input_headers) + [f"scraped_{c}" for c in scraped_cols]

        matched = 0
        output_rows = []
        for row, url in zip(rows, urls):
            out_row = dict(row)
            flat = {c: "" for c in scraped_cols}

            # Replace the original URL column with the cleaned URL
            if url and "linkedin.com/in/" in url:
                out_row[url_col] = url  # Update the original URL column with the clean URL

                profile = all_profiles.get(url)
                if not profile:
                    pub_id = url.rstrip("/").split("/in/")[-1]
                    profile = all_profiles.get(pub_id)
                if profile and "error" not in profile:
                    flat = flatten_profile(profile)
                    matched += 1

            for col in scraped_cols:
                out_row[f"scraped_{col}"] = flat.get(col, "")
            output_rows.append(out_row)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=output_headers)
            writer.writeheader()
            writer.writerows(output_rows)

        jobs[job_id]["status"] = "done"
        jobs[job_id]["matched"] = matched
        jobs[job_id]["output_file"] = output_path

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
    finally:
        # Clean up input file
        if os.path.exists(input_path):
            os.remove(input_path)


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
        "status": "queued",
        "progress": 0,
        "total": 0,
        "matched": 0,
        "output_file": None,
        "error": None,
    }

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


@app.route("/download/<job_id>")
def download(job_id):
    job = jobs.get(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "File not ready"}), 404
    return send_file(
        job["output_file"],
        mimetype="text/csv",
        as_attachment=True,
        download_name="linkedin_enriched.csv",
    )


if __name__ == "__main__":
    os.makedirs("uploads", exist_ok=True)
    os.makedirs("outputs", exist_ok=True)
    port = int(os.getenv("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
