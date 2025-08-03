import asyncio
import logging
import os
import json
import time
from datetime import datetime
from pyppeteer import launch

# Set up logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"coles_realistic_log_{timestamp}.txt"

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])
logger = logging.getLogger("ColesRealistic")

# All Coles categories
CATEGORIES_TO_SCRAPE = [
    {"name": "bakery", "url": "https://www.coles.com.au/browse/bakery"},
    {"name": "fruit-vegetables", "url": "https://www.coles.com.au/browse/fruit-vegetables"},
    {"name": "meat-seafood", "url": "https://www.coles.com.au/browse/meat-seafood"},
    {"name": "dairy-eggs-fridge", "url": "https://www.coles.com.au/browse/dairy-eggs-fridge"},
    {"name": "pantry", "url": "https://www.coles.com.au/browse/pantry"},
    {"name": "frozen", "url": "https://www.coles.com.au/browse/frozen"},
    {"name": "drinks", "url": "https://www.coles.com.au/browse/drinks"},
    {"name": "snacks", "url": "https://www.coles.com.au/browse/chips-chocolates-snacks"},
    {"name": "household", "url": "https://www.coles.com.au/browse/household"},
    {"name": "health-beauty", "url": "https://www.coles.com.au/browse/health-beauty"},
    {"name": "baby", "url": "https://www.coles.com.au/browse/baby"},
    {"name": "pet", "url": "https://www.coles.com.au/browse/pet"},
    {"name": "deli", "url": "https://www.coles.com.au/browse/deli"},
    {"name": "liquor", "url": "https://www.coles.com.au/browse/liquor"}
]

async def scrape_products_from_page(page, category_name):
    """Extract products from the current page"""
    
    products = await page.evaluate('''
        (categoryName) => {
            const products = [];
            const productElements = document.querySelectorAll('.coles-targeting-ProductTileHeaderWrapper');
            
            productElements.forEach((element, index) => {
                try {
                    // Get product ID from URL
                    let productId = "";
                    const productLink = element.querySelector('a[href*="/product/"]');
                    if (productLink && productLink.href) {
                        const urlMatch = productLink.href.match(/product\\/.*-(\\d+)$/);
                        if (urlMatch && urlMatch[1]) {
                            productId = urlMatch[1];
                        }
                    }
                    
                    // Get product title
                    let title = "Unknown Product";
                    const titleElement = element.querySelector('.product__title');
                    if (titleElement) {
                        title = titleElement.textContent.trim();
                    }
                    
                    // Get product URL
                    let productUrl = "";
                    if (productLink && productLink.href) {
                        productUrl = productLink.href;
                    }
                    
                    // Get product image URL
                    let imageUrl = "";
                    const imageElement = element.querySelector('img');
                    if (imageElement && imageElement.src) {
                        imageUrl = imageElement.src;
                    }
                    
                    // Get price value
                    let priceValue = 0;
                    const priceElement = element.querySelector('.price__value');
                    if (priceElement) {
                        const priceText = priceElement.textContent.trim();
                        const priceMatch = priceText.match(/\\$?([\\d\\.]+)/);
                        if (priceMatch && priceMatch[1]) {
                            priceValue = parseFloat(priceMatch[1]);
                        }
                    }
                    
                    // Get unit price
                    let unitPrice = "";
                    const unitPriceElement = element.querySelector('.price__calculation_method');
                    if (unitPriceElement) {
                        unitPrice = unitPriceElement.textContent.split('|')[0].trim();
                    }
                    
                    // Get was price (original price)
                    let wasPriceValue = 0;
                    const wasPriceElement = element.querySelector('.price__was');
                    if (wasPriceElement) {
                        const wasPriceText = wasPriceElement.textContent.trim();
                        const wasPriceMatch = wasPriceText.match(/\\$?([\\d\\.]+)/);
                        if (wasPriceMatch && wasPriceMatch[1]) {
                            wasPriceValue = parseFloat(wasPriceMatch[1]);
                        }
                    }
                    
                    // Calculate savings
                    let saveValue = 0;
                    if (wasPriceValue > priceValue && priceValue > 0) {
                        saveValue = wasPriceValue - priceValue;
                    }
                    
                    // Special badge text
                    let specialText = "";
                    const specialElement = element.querySelector('.roundel-text');
                    if (specialElement) {
                        specialText = specialElement.textContent.trim();
                    }
                    
                    // Add to products array
                    products.push({
                        id: productId,
                        title: title,
                        price_value: priceValue,
                        was_price_value: wasPriceValue,
                        save_value: saveValue,
                        unit_price: unitPrice,
                        url: productUrl,
                        image_url: imageUrl,
                        category: categoryName,
                        special_text: specialText,
                        vendor: "coles",
                        scrapedAt: new Date().toISOString()
                    });
                } catch (error) {
                    console.error(`Error processing product ${index}:`, error);
                }
            });
            
            return products;
        }
    ''', category_name)
    
    return products

async def scrape_category_aggressively(browser, category_info, max_attempts=20):
    """Scrape category by trying multiple page numbers aggressively"""
    category_name = category_info["name"]
    url = category_info["url"]
    
    logger.info(f"Starting aggressive scrape of: {category_name}")
    
    all_products = []
    
    try:
        page = await browser.newPage()
        
        # Set user agent
        await page.setUserAgent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
        
        await page.setViewport({'width': 1920, 'height': 1080})
        
        # Navigate to main page first
        try:
            await page.goto("https://www.coles.com.au/", {"waitUntil": "networkidle0", "timeout": 30000})
            await asyncio.sleep(2)
        except:
            pass
        
        # Try to scrape up to max_attempts pages
        consecutive_empty_pages = 0
        
        for page_num in range(1, max_attempts + 1):
            if page_num == 1:
                page_url = url
            else:
                page_url = f"{url}?page={page_num}"
            
            logger.info(f"Attempting page {page_num} for {category_name}: {page_url}")
            
            try:
                await page.goto(page_url, {"waitUntil": "networkidle0", "timeout": 30000})
                await asyncio.sleep(5)
                
                # Check if page loaded successfully
                page_title = await page.title()
                if not page_title or "error" in page_title.lower():
                    logger.warning(f"Page {page_num} may have failed to load properly")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= 3:
                        logger.info(f"3 consecutive empty pages, stopping {category_name}")
                        break
                    continue
                
                # Extract products
                page_products = await scrape_products_from_page(page, category_name)
                
                if page_products and len(page_products) > 0:
                    # Add page number to products
                    for product in page_products:
                        product['page'] = page_num
                    
                    all_products.extend(page_products)
                    consecutive_empty_pages = 0
                    logger.info(f"✅ Page {page_num}: Found {len(page_products)} products (total: {len(all_products)})")
                    
                    # Sample products from this page
                    if page_products:
                        sample = page_products[0]
                        logger.info(f"   Sample: {sample.get('title', 'Unknown')[:40]}... - ${sample.get('price_value', 0)}")
                else:
                    consecutive_empty_pages += 1
                    logger.info(f"❌ Page {page_num}: No products found")
                    
                    if consecutive_empty_pages >= 3:
                        logger.info(f"3 consecutive empty pages, stopping {category_name}")
                        break
                
                # Wait between page requests
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Error on page {page_num} for {category_name}: {e}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 3:
                    break
        
        await page.close()
        logger.info(f"Completed {category_name}: {len(all_products)} total products")
        
        return all_products
        
    except Exception as e:
        logger.error(f"Error scraping {category_name}: {e}")
        return []

async def main():
    logger.info("=" * 60)
    logger.info("COLES REALISTIC SCRAPER STARTED")
    logger.info("Trying aggressive pagination to find maximum products")
    logger.info("=" * 60)
    
    output_dir = "coles_realistic_data"
    os.makedirs(output_dir, exist_ok=True)
    
    total_products = 0
    successful_categories = 0
    all_results = {}
    
    browser = await launch(
        headless=True,
        args=[
            '--no-sandbox', 
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--window-size=1920,1080',
            '--disable-blink-features=AutomationControlled'
        ]
    )
    
    try:
        for i, category_info in enumerate(CATEGORIES_TO_SCRAPE, 1):
            category_name = category_info["name"]
            
            logger.info(f"\n[{i}/{len(CATEGORIES_TO_SCRAPE)}] Processing: {category_name}")
            
            # Try aggressive scraping
            products = await scrape_category_aggressively(browser, category_info, max_attempts=25)
            
            if products:
                successful_categories += 1
                total_products += len(products)
                all_results[category_name] = products
                
                # Save results
                category_filename = os.path.join(output_dir, f"coles_{category_name}_{timestamp}.json")
                category_data = {
                    "metadata": {
                        "category": category_name,
                        "url": category_info["url"],
                        "timestamp": datetime.now().isoformat(),
                        "total_products": len(products),
                        "vendor": "coles",
                        "pages_scraped": max(product.get('page', 1) for product in products) if products else 0
                    },
                    "products": products
                }
                
                with open(category_filename, 'w', encoding='utf-8') as f:
                    json.dump(category_data, f, indent=2, ensure_ascii=False)
                
                logger.info(f"💾 Saved {len(products)} products to {category_filename}")
            else:
                logger.warning(f"❌ No products found for {category_name}")
            
            # Wait between categories
            if i < len(CATEGORIES_TO_SCRAPE):
                logger.info("Waiting 10 seconds before next category...")
                await asyncio.sleep(10)
    
    finally:
        await browser.close()
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("REALISTIC SCRAPING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total categories attempted: {len(CATEGORIES_TO_SCRAPE)}")
    logger.info(f"Successful categories: {successful_categories}")
    logger.info(f"TOTAL PRODUCTS FOUND: {total_products}")
    
    if all_results:
        logger.info("\nDetailed results:")
        for category, products in all_results.items():
            pages_scraped = max(product.get('page', 1) for product in products) if products else 0
            logger.info(f"  {category:20}: {len(products):4d} products ({pages_scraped:2d} pages)")
    
    # Save summary
    summary_filename = os.path.join(output_dir, f"coles_realistic_summary_{timestamp}.json")
    summary_data = {
        "run_timestamp": datetime.now().isoformat(),
        "categories_attempted": len(CATEGORIES_TO_SCRAPE),
        "categories_successful": successful_categories,
        "total_products": total_products,
        "results_by_category": {cat: len(prods) for cat, prods in all_results.items()},
        "pages_by_category": {cat: max(product.get('page', 1) for product in prods) if prods else 0 for cat, prods in all_results.items()},
        "log_file": log_filename
    }
    
    with open(summary_filename, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2)
    
    logger.info(f"\nSummary saved to: {summary_filename}")
    logger.info(f"Log file: {log_filename}")
    logger.info("REALISTIC SCRAPING COMPLETED")

if __name__ == "__main__":
    asyncio.run(main())