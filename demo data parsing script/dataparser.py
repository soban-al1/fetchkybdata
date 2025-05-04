#!/usr/bin/env python3
"""
company_scraper.py

A semi-automated demo script that:
1. Fetches latest announcements from SGX and (optionally) Bursa Malaysia.
2. Filters by a user-provided company name.
3. Downloads attachments (PDF/HTML).
4. Extracts text and key lines (shareholder/director/board mentions).
5. Outputs a JSON file for downstream ingestion.

Usage:
    python company_scraper.py --company-name "Your Company" [--bursa-code 1234]
"""

import os
import re
import json
import argparse
import requests
from bs4 import BeautifulSoup
import pdfplumber

# ——— ARGPARSE ————————————————————————————————————————————————————————————————
parser = argparse.ArgumentParser(description="Scrape SGX & Bursa announcements for a given company.")
parser.add_argument(
    "--company-name", "-c",
    required=True,
    help="Company name to filter announcement titles (case-insensitive)"
)
parser.add_argument(
    "--bursa-code", "-b",
    type=str,
    default=None,
    help="(Optional) Bursa Malaysia company code (e.g. 5183). If provided, will fetch that company’s announcements."
)
args = parser.parse_args()
COMPANY_NAME = args.company_name.strip()
COMPANY_SLUG = re.sub(r"\W+", "_", COMPANY_NAME.lower())
BURSA_CODE = args.bursa_code

# ——— CONFIG —————————————————————————————————————————————————————————————————
SGX_ANNOUNCE_URL   = "https://www.sgx.com/announcements?t=latest"
BURSA_BASE_URL     = "https://www.bursamalaysia.com"
BURSA_ANNOUNCE_URL = (
    f"{BURSA_BASE_URL}/market_information/announcements/company_announcement"

) if BURSA_CODE else None

DOWNLOAD_DIR = "downloads"
OUTPUT_JSON  = f"{COMPANY_SLUG}_announcements.json"

# compile once for faster filtering
KEYWORDS = re.compile(re.escape(COMPANY_NAME), re.IGNORECASE)


# ——— FETCH FUNCTIONS —————————————————————————————————————————————————————————————
def fetch_sgx_announcements():
    """Fetch SGX announcements and return list of dicts."""
    resp = requests.get(SGX_ANNOUNCE_URL, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    anns = []
    for item in soup.select(".views-row"):
        a = item.find("a")
        date_tag = item.select_one(".views-date") or item.select_one("time")
        if not a or not a.get("href"):
            continue
        link = a["href"]
        if not link.startswith("http"):
            link = "https://www.sgx.com" + link
        anns.append({
            "source": "SGX",
            "title": a.get_text(strip=True),
            "link": link,
            "date": date_tag.get_text(strip=True) if date_tag else ""
        })
    return anns


def fetch_bursa_announcements():
    """Fetch Bursa announcements for a given code, avoiding 403 via browser emulation."""
    if not BURSA_ANNOUNCE_URL:
        return []

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.5735.110 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BURSA_BASE_URL + "/",
        "Connection": "keep-alive",
    })

    # get cookies
    session.get(BURSA_BASE_URL + "/").raise_for_status()

    resp = session.get(BURSA_ANNOUNCE_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    anns = []
    for row in soup.select("table.data-table tbody tr"):
        cols = row.find_all("td")
        if len(cols) < 2:
            continue
        date = cols[0].get_text(strip=True)
        link_tag = cols[1].find("a")
        if not link_tag or not link_tag.get("href"):
            continue
        link = link_tag["href"]
        if not link.startswith("http"):
            link = BURSA_BASE_URL + link
        anns.append({
            "source": "Bursa",
            "date": date,
            "title": link_tag.get_text(strip=True),
            "link": link,
        })
    return anns


# ——— FILTER, DOWNLOAD & PARSE —————————————————————————————————————————————————————
def filter_announcements(anns):
    """Keep only those where title contains the company name."""
    return [a for a in anns if KEYWORDS.search(a["title"])]


def download_file(ann):
    """Download an announcement and return the local file path."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    r = requests.get(ann["link"], stream=True, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    fn = ann["link"].split("/")[-1] or f"{ann['date']}_{ann['title'][:30]}.html"
    fn = re.sub(r"[\\/*?\"<>|]", "_", fn)
    path = os.path.join(DOWNLOAD_DIR, fn)

    with open(path, "wb") as f:
        for chunk in r.iter_content(4096):
            if chunk:
                f.write(chunk)
    return path


def extract_text(path):
    """Extract text from PDF (via pdfplumber) or read HTML/text file."""
    text = ""
    if path.lower().endswith(".pdf"):
        try:
            with pdfplumber.open(path) as pdf:
                for pg in pdf.pages:
                    pg_txt = pg.extract_text()
                    if pg_txt:
                        text += pg_txt + "\n"
        except Exception as e:
            print(f"  [!] PDF parse error: {e}")
    else:
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
        except Exception as e:
            print(f"  [!] Read error: {e}")
    return text


def extract_key_lines(text, max_lines=5):
    """Return up to max_lines that mention shareholder/director/board or the company name."""
    kws = re.compile(r"(?i)shareholder|director|board|" + re.escape(COMPANY_NAME))
    lines = text.splitlines()
    matches = [ln.strip() for ln in lines if kws.search(ln)]
    return matches[:max_lines]


# ——— MAIN —————————————————————————————————————————————————————————————————————
def main():
    print(f"→ Scraping for company: {COMPANY_NAME!r}\n")

    sgx = fetch_sgx_announcements()
    print(f"SGX: fetched {len(sgx)} announcements")

    bursa = fetch_bursa_announcements()
    if BURSA_CODE:
        print(f"Bursa: fetched {len(bursa)} announcements for code {BURSA_CODE}")
    else:
        print("Bursa: skipped (no code provided)")

    all_anns = sgx + bursa
    filtered = filter_announcements(all_anns)
    print(f"Filtered to {len(filtered)} announcements mentioning {COMPANY_NAME!r}\n")

    results = []
    for ann in filtered:
        print(f"• {ann['date']} | {ann['source']} | {ann['title']}")
        try:
            path = download_file(ann)
            text = extract_text(path)
            kl   = extract_key_lines(text)
            results.append({
                "date":     ann["date"],
                "source":   ann["source"],
                "title":    ann["title"],
                "link":     ann["link"],
                "file":     os.path.basename(path),
                "key_lines": kl
            })
            print(f"   → downloaded & extracted {len(kl)} key lines")
        except Exception as e:
            print(f"   [!] error: {e}")

    # write JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nDone. Results written to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
