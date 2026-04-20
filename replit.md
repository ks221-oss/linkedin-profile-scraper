# LinkedIn Profile Enricher

## Overview
A Flask web application that enriches LinkedIn profile data using the Apify API. Users upload a CSV file containing LinkedIn URLs, and the app scrapes detailed professional information (job titles, companies, education, skills, contact details) and returns an enriched CSV.

## Tech Stack
- **Language:** Python 3.12
- **Web Framework:** Flask
- **Production Server:** Gunicorn
- **External API:** Apify (`dev_fusion~Linkedin-Profile-Scraper` actor)
- **Environment:** python-dotenv for secrets management

## Project Structure
```
.
├── app.py                    # Main Flask app with routes and background job processing
├── scraper.py                # CLI scraper tool
├── requirements.txt          # Python dependencies
├── Procfile                  # Legacy deployment config
├── templates/index.html      # Single-page frontend (drag-and-drop CSV upload)
├── uploads/                  # Temporary uploaded CSVs (created at runtime)
├── outputs/                  # Enriched CSV output files
├── sample_profiles.csv       # Example input
└── sample_profiles_enriched.csv  # Example output
```

## Configuration
- **Port:** 5000 (0.0.0.0)
- **Workflow:** `Start application` → `python app.py`
- **Deployment:** VM target (uses in-memory job state) → `gunicorn --bind=0.0.0.0:5000 --reuse-port app:app`

## Environment Variables
- `APIFY_API_TOKEN` — Required for scraping LinkedIn profiles via Apify

## Key Features
- CSV upload with auto-detection of LinkedIn URL column
- Background thread processing with real-time progress polling
- Batch processing (10 profiles per batch)
- URL normalization (strips query params)
- Enriches original CSV with 25+ scraped fields prefixed with `scraped_`
- Download enriched CSV when job completes

## Running the App
```bash
python app.py
```
Or via workflow: "Start application"
