import sys
import csv
import json
import time
import requests
from dotenv import load_dotenv
import os

load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")
ACTOR_ID = "dev_fusion~Linkedin-Profile-Scraper"
BASE_URL = "https://api.apify.com/v2"

# Max profiles per Apify actor run (they support batching)
BATCH_SIZE = 10


def normalize_linkedin_url(url: str) -> str:
    """Strip query params and fragments from a LinkedIn URL.
    e.g. https://www.linkedin.com/in/user/?originalSubdomain=in  →  https://www.linkedin.com/in/user/
    """
    url = url.strip()
    # Remove everything from ? or # onwards
    for char in ("?", "#"):
        if char in url:
            url = url[:url.index(char)]
    # Ensure trailing slash
    if not url.endswith("/"):
        url += "/"
    return url


def scrape_profiles(linkedin_urls: list[str]) -> list[dict]:
    """Run the Apify actor with a batch of URLs and return scraped profiles."""

    run_url = f"{BASE_URL}/acts/{ACTOR_ID}/runs"
    payload = {"profileUrls": linkedin_urls}
    headers = {"Content-Type": "application/json"}
    params = {"token": APIFY_TOKEN}

    print(f"\nScraping batch of {len(linkedin_urls)} profiles...")
    for url in linkedin_urls:
        print(f"  - {url}")

    resp = requests.post(run_url, json=payload, headers=headers, params=params)
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]

    # Poll until the run finishes
    status_url = f"{BASE_URL}/actor-runs/{run_id}"
    while True:
        resp = requests.get(status_url, params=params)
        resp.raise_for_status()
        run_info = resp.json()["data"]
        status = run_info["status"]
        if status == "SUCCEEDED":
            print("  Batch completed!")
            break
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"  Batch failed with status: {status}")
            return []
        print(f"  Status: {status} — waiting...")
        time.sleep(3)

    # Fetch results
    dataset_id = run_info["defaultDatasetId"]
    items_url = f"{BASE_URL}/datasets/{dataset_id}/items"
    resp = requests.get(items_url, params=params)
    resp.raise_for_status()
    return resp.json()


def scrape_single(linkedin_url: str) -> dict:
    """Scrape a single profile (convenience wrapper)."""
    results = scrape_profiles([normalize_linkedin_url(linkedin_url)])
    return results[0] if results else {}


def flatten_profile(profile: dict) -> dict:
    """Flatten a profile dict into a single-level dict for CSV output."""
    if not profile or "error" in profile:
        return {"error": profile.get("error", "No data returned")}

    # Experience: combine into text
    experiences = profile.get("experiences") or []
    exp_lines = []
    for job in experiences:
        title = job.get("title", "")
        company = job.get("companyName", "")
        started = job.get("jobStartedOn", "")
        ended = job.get("jobEndedOn") or ("Present" if job.get("jobStillWorking") else "")
        period = f" ({started} - {ended})" if started else ""
        exp_lines.append(f"{title} @ {company}{period}")

    # Education: combine into text
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

    # Skills
    skills = profile.get("skills") or []
    skill_names = [s.get("title", str(s)) if isinstance(s, dict) else str(s) for s in skills]

    return {
        "linkedin_url": profile.get("linkedinUrl", ""),
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
        "is_job_seeker": profile.get("isJobSeeker", ""),
        "is_creator": profile.get("isCreator", ""),
        "experiences": " | ".join(exp_lines),
        "education": " | ".join(edu_lines),
        "skills": ", ".join(skill_names),
    }


# ── CSV Mode ──────────────────────────────────────────────

def find_url_column(headers: list[str]) -> str:
    """Auto-detect which column contains LinkedIn URLs."""
    for h in headers:
        if "linkedin" in h.lower() or "url" in h.lower() or "link" in h.lower() or "profile" in h.lower():
            return h
    # Fallback: check first row values later
    return None


def process_csv(input_file: str, output_file: str):
    """Read a CSV of LinkedIn URLs, scrape them, write enriched CSV."""

    # Read input CSV
    with open(input_file, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        input_headers = reader.fieldnames or []
        rows = list(reader)

    if not rows:
        print("Error: Input CSV is empty.")
        sys.exit(1)

    # Detect the URL column
    url_col = find_url_column(input_headers)
    if not url_col:
        # Try first column
        url_col = input_headers[0]
        print(f"Could not auto-detect URL column, using first column: '{url_col}'")
    else:
        print(f"Detected URL column: '{url_col}'")

    # Extract URLs
    urls = []
    for row in rows:
        url = (row.get(url_col) or "").strip()
        if url and "linkedin.com/in/" in url:
            urls.append(normalize_linkedin_url(url))
        else:
            urls.append(None)

    valid_urls = [u for u in urls if u]
    print(f"\nFound {len(valid_urls)} valid LinkedIn URLs out of {len(rows)} rows.")

    if not valid_urls:
        print("Error: No valid LinkedIn profile URLs found in the CSV.")
        sys.exit(1)

    # Scrape in batches
    all_profiles = {}  # url -> profile dict
    for i in range(0, len(valid_urls), BATCH_SIZE):
        batch = valid_urls[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(valid_urls) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"\n── Batch {batch_num}/{total_batches} ──")
        results = scrape_profiles(batch)
        for profile in results:
            profile_url = profile.get("linkedinUrl", "")
            # Match by public identifier since URLs may differ slightly
            pub_id = profile.get("publicIdentifier", "")
            all_profiles[profile_url] = profile
            if pub_id:
                all_profiles[pub_id] = profile

    # Build output CSV
    # Get the scraped field names from flatten
    sample_flat = flatten_profile({"fullName": "sample"})
    scraped_columns = [k for k in sample_flat.keys() if k != "linkedin_url"]

    # Output headers = original columns + scraped columns (skip duplicating the URL)
    output_headers = list(input_headers) + [f"scraped_{col}" for col in scraped_columns]

    output_rows = []
    matched = 0
    for row, url in zip(rows, urls):
        out_row = dict(row)
        if url:
            # Try to find the profile by URL or public identifier
            profile = all_profiles.get(url)
            if not profile:
                # Try matching by extracting the public ID from the input URL
                pub_id = url.rstrip("/").split("/in/")[-1] if "/in/" in url else ""
                for key, p in all_profiles.items():
                    if p.get("publicIdentifier", "") == pub_id:
                        profile = p
                        break
            if profile and "error" not in profile:
                flat = flatten_profile(profile)
                matched += 1
            else:
                flat = {k: "" for k in scraped_columns}
                flat["error"] = profile.get("error", "No data") if profile else "No data"
                scraped_columns_with_error = scraped_columns if "error" in scraped_columns else scraped_columns
        else:
            flat = {k: "" for k in scraped_columns}

        for col in scraped_columns:
            out_row[f"scraped_{col}"] = flat.get(col, "")

        output_rows.append(out_row)

    # Write output
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_headers)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"\n{'=' * 60}")
    print(f"DONE! Scraped {matched}/{len(valid_urls)} profiles successfully.")
    print(f"Output saved to: {output_file}")
    print(f"{'=' * 60}")


# ── Single URL Mode ───────────────────────────────────────

def display_profile(profile: dict):
    """Pretty-print the scraped profile data."""
    if not profile:
        return

    if "error" in profile:
        print(f"\nError: {profile['error']}")
        return

    print("\n" + "=" * 60)
    print("LINKEDIN PROFILE")
    print("=" * 60)

    fields = [
        ("Name", lambda p: p.get("fullName") or f"{p.get('firstName', '')} {p.get('lastName', '')}".strip()),
        ("Headline", lambda p: p.get("headline")),
        ("Current Role", lambda p: f"{p.get('jobTitle', '')} @ {p.get('companyName', '')}".strip(" @") or None),
        ("Location", lambda p: p.get("addressWithCountry")),
        ("About", lambda p: p.get("about")),
        ("Email", lambda p: p.get("email")),
        ("Phone", lambda p: p.get("mobileNumber")),
        ("Profile URL", lambda p: p.get("linkedinUrl")),
        ("Connections", lambda p: p.get("connections")),
        ("Followers", lambda p: p.get("followers")),
        ("Total Experience", lambda p: f"{p.get('totalExperienceYears')} years" if p.get("totalExperienceYears") else None),
        ("Premium", lambda p: "Yes" if p.get("isPremium") else None),
        ("Verified", lambda p: "Yes" if p.get("isVerified") else None),
    ]

    for label, getter in fields:
        value = getter(profile)
        if value:
            print(f"\n{label}: {value}")

    experiences = profile.get("experiences") or []
    if experiences:
        print(f"\n{'─' * 40}")
        print("EXPERIENCE")
        print("─" * 40)
        for job in experiences:
            title = job.get("title", "N/A")
            company = job.get("companyName", "N/A")
            started = job.get("jobStartedOn", "")
            ended = job.get("jobEndedOn") or ("Present" if job.get("jobStillWorking") else "")
            location = job.get("jobLocation", "")
            emp_type = job.get("employmentType", "")
            print(f"\n  {title} @ {company}")
            if started:
                print(f"  Period: {started} — {ended}")
            if location:
                print(f"  Location: {location}")
            if emp_type:
                print(f"  Type: {emp_type}")

    educations = profile.get("educations") or []
    if educations:
        print(f"\n{'─' * 40}")
        print("EDUCATION")
        print("─" * 40)
        for edu in educations:
            school = edu.get("title", "N/A")
            degree = edu.get("subtitle", "")
            period = edu.get("period", {})
            start_year = period.get("startedOn", {}).get("year", "")
            end_year = period.get("endedOn", {}).get("year", "")
            print(f"\n  {school}")
            if degree:
                print(f"  {degree}")
            if start_year or end_year:
                print(f"  {start_year} — {end_year}")

    skills = profile.get("skills") or []
    if skills:
        print(f"\n{'─' * 40}")
        print("SKILLS")
        print("─" * 40)
        skill_names = [s.get("title", str(s)) if isinstance(s, dict) else str(s) for s in skills]
        print(f"  {', '.join(skill_names)}")

    print("\n" + "=" * 60)

    out_file = "profile_output.json"
    with open(out_file, "w") as f:
        json.dump(profile, f, indent=2)
    print(f"\nRaw JSON saved to: {out_file}")


# ── Main ──────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("LinkedIn Profile Scraper")
        print("=" * 40)
        print("\nUsage:")
        print("  Single profile:  python3 scraper.py <linkedin-url>")
        print("  CSV batch:       python3 scraper.py <input.csv> [output.csv]")
        print("\nExamples:")
        print("  python3 scraper.py https://www.linkedin.com/in/williamhgates")
        print("  python3 scraper.py profiles.csv")
        print("  python3 scraper.py profiles.csv enriched_profiles.csv")
        sys.exit(1)

    if not APIFY_TOKEN:
        print("Error: APIFY_API_TOKEN not found. Make sure your .env file is set up.")
        sys.exit(1)

    arg = sys.argv[1]

    if arg.endswith(".csv"):
        # CSV batch mode
        input_file = arg
        if not os.path.exists(input_file):
            print(f"Error: File not found: {input_file}")
            sys.exit(1)
        output_file = sys.argv[2] if len(sys.argv) > 2 else input_file.replace(".csv", "_enriched.csv")
        process_csv(input_file, output_file)
    else:
        # Single URL mode
        if "linkedin.com/in/" not in arg:
            print("Error: Please provide a valid LinkedIn profile URL (containing linkedin.com/in/)")
            sys.exit(1)
        profile = scrape_single(arg)
        display_profile(profile)
