# Supermarket Product Scrapers

A Python web scraping suite for collecting product data from Australian supermarket websites (Woolworths and Coles) for price comparison and consumer research.

## Features

- **Woolworths Scraper**: Comprehensive product data collection from all categories
- **Coles Scraper**: Product data collection with comparison capabilities
- **Shared Utilities**: Common database, duplicate detection, and analysis tools
- Removes duplicate products within and across categories
- Supports headless and non-headless browser modes
- Optional integration with Supabase for data storage
- Generates detailed reports and statistics

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

### Woolworths Scraper

```bash
# Basic scraping (all categories)
python -m woolworths.woolworths_scraper

# Scraping specific categories
python -m woolworths.woolworths_scraper --categories fruit-veg,bakery

# With Supabase upload
python -m woolworths.woolworths_scraper --upload

# Limit pages per category
python -m woolworths.woolworths_scraper --max-pages 5
```

### Coles Scraper

```bash
# Basic scraping
python -m coles.coles_scraper

# With upload
python -m coles.coles_scraper --upload
```

## Output

- Product data saved as JSON files in respective `*_data/` directories
- Summary reports with statistics
- Duplicate analysis reports
- Cross-category duplicate detection

## Project Structure

```
├── woolworths/           # Woolworths-specific scraping
│   ├── woolworths_scraper.py
│   └── product_processor.py
├── coles/               # Coles-specific scraping
│   ├── coles_scraper.py
│   └── coles_product_processor.py
├── shared/              # Shared utilities
│   ├── db_utils.py      # Database integration
│   ├── count_products.py
│   ├── duplicates.py
│   └── find_duplicate_products.py
├── woolworths_data/     # Woolworths output (ignored by git)
├── coles/coles_data/    # Coles output (ignored by git)
└── improvements.md      # Development notes
```

## Notes

This tool is intended for price comparison and consumer research purposes only.