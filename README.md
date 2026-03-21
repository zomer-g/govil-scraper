# Gov.il Scraper

Scrapes Israeli government portal (gov.il) collector pages via internal JSON APIs. Includes a Flask dashboard for managing scrape jobs, with CSV/Excel export and file attachment downloads.

## Features

- Scrapes gov.il collector pages by URL
- Detects page type and extracts structured data
- Downloads file attachments (documents, PDFs, images)
- Real-time progress tracking via Server-Sent Events (SSE)
- Export to CSV and Excel (openpyxl)
- Web dashboard with job management
- Google OAuth2 SSO for admin access
- Rate limiting and concurrent job control

## Tech Stack

- **Backend:** Python, Flask, Gunicorn
- **Scraping:** Requests, CloudScraper, Playwright (optional)
- **Export:** openpyxl (Excel), CSV
- **Auth:** Google OAuth2 via Authlib
- **Deployment:** Docker, Render

## Getting Started

```bash
# Install dependencies
pip install -r requirements.txt

# Optional: install Playwright for JS-rendered pages
playwright install chromium

# Run the server
python app.py
```

## Deployment

Configured for Render via `render.yaml`. See `Dockerfile` for containerized deployment.
