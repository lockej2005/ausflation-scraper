# Woolworths Product Scraper

A Python web scraper for collecting product data from Woolworths online store, with comparison capabilities for Coles products.

## Features

- Scrapes product information from multiple Woolworths categories
- Removes duplicate products within and across categories
- Supports headless and non-headless browser modes
- Optional integration with Supabase for data storage
- Generates detailed reports and statistics
- Includes Coles product comparison functionality

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables (optional, for Supabase integration):
```bash
export SUPABASE_URL="your_supabase_url"
export SUPABASE_KEY="your_supabase_key"
```

## Usage

### Basic scraping (all categories):
```bash
python woolworths_scraper.py
```

### Scraping specific categories:
```bash
python woolworths_scraper.py --categories fruit-veg,bakery
```

### With Supabase upload:
```bash
python woolworths_scraper.py --upload
```

### Limit pages per category:
```bash
python woolworths_scraper.py --max-pages 5
```

## Output

- Product data saved as JSON files in `woolworths_data/` directory
- Summary reports with statistics
- Duplicate analysis reports
- Cross-category duplicate detection

## Project Structure

- `woolworths_scraper.py` - Main scraper script
- `product_processor.py` - Product data processing utilities
- `db_utils.py` - Database integration utilities
- `coles/` - Coles comparison functionality
- `woolworths_data/` - Output directory (ignored by git)

## Notes

This tool is intended for price comparison and consumer research purposes.