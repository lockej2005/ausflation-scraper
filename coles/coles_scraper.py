import asyncio
import logging
import os
import json
import time
from datetime import datetime
import argparse
from pyppeteer import launch
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import utility modules
from shared.db_utils import SupabaseClient
# Import the modified ProductProcessor class
from .coles_product_processor import ColesProductProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ColesScraper")

# Array of Coles category URLs to scrape
CATEGORY_URLS = [
    "https://www.coles.com.au/browse/meat-seafood?pid=homepage_cat_explorer_meat_seafood",
    "https://www.coles.com.au/browse/fruit-vegetables?pid=homepage_cat_explorer_fruit_vege",
    "https://www.coles.com.au/browse/dairy-eggs-fridge?pid=homepage_cat_explorer_dairy_eggs_fridge",
    "https://www.coles.com.au/browse/bakery?pid=homepage_cat_explorer_bakery",
    "https://www.coles.com.au/browse/deli?pid=homepage_cat_explorer_deli",
    "https://www.coles.com.au/browse/pantry?pid=homepage_cat_explorer_pantry",
    "https://www.coles.com.au/browse/dietary-world-foods?pid=homepage_cat_explorer_dietary-world-foods",
    "https://www.coles.com.au/browse/chips-chocolates-snacks?pid=homepage_cat_explorer_chips-chocolates-snacks",
    "https://www.coles.com.au/browse/drinks?pid=homepage_cat_explorer_drinks",
    "https://www.coles.com.au/browse/frozen?pid=homepage_cat_explorer_frozen",
    "https://www.coles.com.au/browse/household?pid=homepage_cat_explorer_household",
    "https://www.coles.com.au/browse/health-beauty?pid=homepage_cat_explorer_health_beauty",
    "https://www.coles.com.au/browse/baby?pid=homepage_cat_explorer_baby",
    "https://www.coles.com.au/browse/pet?pid=homepage_cat_explorer_pet",
    "https://www.coles.com.au/browse/liquor?pid=homepage_cat_explorer_liquor"
]

def find_chrome_path():
    """Find Chrome or Edge installation on the system"""
    possible_paths = []
    
    if os.name == 'nt':  # Windows
        possible_paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"
        ]
    
    for path in possible_paths:
        if os.path.exists(path):
            logger.info(f"Found browser at: {path}")
            return path
    
    logger.warning("No Chrome or Edge installation found")
    return None

async def get_total_pages(page):
    """Extract the total number of pages from the pagination element"""
    try:
        total_pages = await page.evaluate('''
            () => {
                // Look for the pagination element using the specific Coles structure
                const paginationItems = document.querySelectorAll('.coles-targeting-PaginationPaginationItem');
                if (paginationItems && paginationItems.length > 0) {
                    // Get all page numbers
                    const pageNumbers = [];
                    paginationItems.forEach(el => {
                        // Extract page numbers from spans with role="button"
                        if (el.getAttribute('role') === 'button') {
                            const pageText = el.textContent.trim();
                            if (!isNaN(pageText)) {
                                pageNumbers.push(parseInt(pageText, 10));
                            }
                        }
                    });
                    
                    // Return the maximum page number
                    if (pageNumbers.length > 0) {
                        return Math.max(...pageNumbers);
                    }
                }
                
                // Alternative method - look for last page button directly
                const lastPageButton = document.querySelector('span[id^="pagination-button-page-"]:last-of-type');
                if (lastPageButton) {
                    const pageNum = lastPageButton.textContent.trim();
                    if (!isNaN(pageNum)) {
                        return parseInt(pageNum, 10);
                    }
                }
                
                // Try finding links to pages
                const pageLinks = document.querySelectorAll('a[href*="&page="]');
                if (pageLinks && pageLinks.length > 0) {
                    const pageNumbers = [];
                    pageLinks.forEach(link => {
                        const match = link.href.match(/[&?]page=(\\d+)/);
                        if (match && match[1]) {
                            pageNumbers.push(parseInt(match[1], 10));
                        }
                    });
                    
                    if (pageNumbers.length > 0) {
                        return Math.max(...pageNumbers);
                    }
                }
                
                return null;
            }
        ''')
        
        if total_pages:
            logger.info(f"Found total pages: {total_pages}")
            return total_pages
    except Exception as e:
        logger.warning(f"Could not extract total pages: {e}")
    
    # If we couldn't find the page indicator, fall back to a default
    return None

async def scrape_products_from_page(page, category_name, total_products_count=None, products_found_so_far=0):
    """Extract products from the current page"""
    
    # First, try to get the total products count if not provided
    if total_products_count is None:
        try:
            total_products_count = await page.evaluate('''
                () => {
                    // Try to find the element that shows product count (e.g., "Showing 1-48 of 123 products")
                    const countElements = document.querySelectorAll('p');
                    
                    for (const element of countElements) {
                        const text = element.textContent || '';
                        // Look for text patterns like "Showing X-Y of Z products"
                        const match = text.match(/of\\s+(\\d+)\\s+products/i) || 
                                      text.match(/(\\d+)\\s+products/i);
                        if (match && match[1]) {
                            return parseInt(match[1], 10);
                        }
                    }
                    
                    // Alternative: try to count the number of product tiles on the page
                    const productTiles = document.querySelectorAll('.coles-targeting-ProductTileHeaderWrapper');
                    if (productTiles && productTiles.length > 0) {
                        // We can't determine the exact total, but at least return the count on this page
                        console.log(`Found ${productTiles.length} products on this page`);
                        return productTiles.length;
                    }
                    
                    return null;
                }
            ''')
            if total_products_count:
                logger.info(f"Found total products count: {total_products_count}")
        except Exception as e:
            logger.warning(f"Could not extract total products count: {e}")
    
    # Extract all products from the page
    products = await page.evaluate('''
        (categoryName) => {
            const products = [];
            
            // Check if the "no products" message is present
            const bodyText = document.body.innerText;
            if (bodyText.includes("No products found") ||
                bodyText.includes("0 products")) {
                return "NO_PRODUCTS";
            }
            
            // Find all product tiles
            const productElements = document.querySelectorAll('.coles-targeting-ProductTileHeaderWrapper');
            
            console.log(`Found ${productElements.length} product elements on this page`);
            
            // If no products found, check if we're at the end
            if (productElements.length === 0) {
                return "NO_PRODUCTS";
            }
            
            // Process each product element
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
                    } else if (imageElement && imageElement.srcset) {
                        // Extract the highest resolution image from srcset
                        const srcset = imageElement.srcset;
                        const srcsetParts = srcset.split(',');
                        if (srcsetParts.length > 0) {
                            // Get the last part which typically has the highest resolution
                            const lastPart = srcsetParts[srcsetParts.length - 1].trim();
                            const urlParts = lastPart.split(' ');
                            if (urlParts.length > 0) {
                                imageUrl = urlParts[0];
                            }
                        }
                    }
                    
                    // Get price value
                    let priceValue = 0;
                    const priceElement = element.querySelector('.price__value');
                    if (priceElement) {
                        const priceText = priceElement.textContent.trim();
                        // Extract numeric value (remove $ and convert to number)
                        const priceMatch = priceText.match(/\\$?([\d\.]+)/);
                        if (priceMatch && priceMatch[1]) {
                            priceValue = parseFloat(priceMatch[1]);
                        }
                    }
                    
                    // Get unit price
                    let unitPrice = "";
                    const unitPriceElement = element.querySelector('.price__calculation_method');
                    if (unitPriceElement) {
                        // Get the text content of the first text node, ignoring child elements
                        let unitPriceText = "";
                        for (const node of unitPriceElement.childNodes) {
                            if (node.nodeType === 3) { // Text node
                                unitPriceText = node.textContent.trim();
                                break;
                            }
                        }
                        if (!unitPriceText && unitPriceElement.childNodes.length > 0) {
                            // Fallback to the entire text content
                            unitPriceText = unitPriceElement.textContent.split('|')[0].trim();
                        }
                        unitPrice = unitPriceText;
                    }
                    
                    // Get was price (original price)
                    let wasPriceValue = 0;
                    const wasPriceElement = element.querySelector('.price__was');
                    if (wasPriceElement) {
                        const wasPriceText = wasPriceElement.textContent.trim();
                        // Extract numeric value
                        const wasPriceMatch = wasPriceText.match(/\\$?([\d\.]+)/);
                        if (wasPriceMatch && wasPriceMatch[1]) {
                            wasPriceValue = parseFloat(wasPriceMatch[1]);
                        }
                    }
                    
                    // Get savings amount
                    let saveValue = 0;
                    const saveElement = element.querySelector('[data-testid="badge-wrapper"] .badge-label');
                    if (saveElement && saveElement.textContent.includes("Save")) {
                        const saveText = saveElement.textContent.trim();
                        // Extract numeric value
                        const saveMatch = saveText.match(/\\$?([\d\.]+)/);
                        if (saveMatch && saveMatch[1]) {
                            saveValue = parseFloat(saveMatch[1]);
                        }
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
                    console.error(`Error details: ${error.message}`);
                    products.push({
                        title: `Product ${index + 1}`,
                        price_value: 0,
                        unit_price: "",
                        category: categoryName,
                        vendor: "coles",
                        error: error.message
                    });
                }
            });
            
            // Debug information
            console.log(`Processed ${products.length} products from ${categoryName}`);
            
            return {
                products: products,
                section: "Main product listing"
            };
        }
    ''', category_name)
    
    # Check if we've reached the end of products
    if products == "NO_PRODUCTS":
        return "NO_PRODUCTS"
    
    return products

async def scrape_category(browser, url, max_pages=None):
    """Scrape all products from a category, handling pagination"""
    # Extract category name from URL
    category_match = re.search(r'browse/([^?]+)', url)
    category_name = category_match.group(1) if category_match else "unknown"
    
    logger.info(f"Scraping category: {category_name} from {url}")
    all_products = []
    total_products_count = None
    
    try:
        # Open new page
        page = await browser.newPage()
        
        # Set a realistic user agent
        await page.setUserAgent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
        
        # Navigate to the first page to get total pages
        first_page_url = url
        logger.info(f"Loading first page to determine total pages: {first_page_url}")
        
        try:
            await page.goto(first_page_url, {"waitUntil": "networkidle0", "timeout": 60000})
            await asyncio.sleep(3)  # Wait for page to load
        except Exception as e:
            logger.error(f"Error navigating to {first_page_url}: {e}")
            try:
                logger.info(f"Retrying first page")
                await page.goto(first_page_url, {"waitUntil": "networkidle0", "timeout": 90000})
                await asyncio.sleep(5)  # Wait longer for retry
            except Exception as e2:
                logger.error(f"Error on retry: {e2}")
                return []
        
        # Get total pages from the pagination element
        total_pages = await get_total_pages(page)
        
        if not total_pages:
            logger.warning("Could not determine total pages, will use default of 10")
            total_pages = 10
        else:
            logger.info(f"Will scrape {total_pages} pages for category {category_name}")
            
            # If max_pages is set, use the smaller of the two
            if max_pages and max_pages < total_pages:
                total_pages = max_pages
                logger.info(f"Limiting to {max_pages} pages as specified")
        
        # For each page, scrape products
        for page_num in range(1, total_pages + 1):
            # Navigate to the page
            page_url = f"{url.split('?')[0]}?page={page_num}"
            logger.info(f"Loading page {page_num}: {page_url}")
            
            try:
                await page.goto(page_url, {"waitUntil": "networkidle0", "timeout": 60000})
                await asyncio.sleep(3)  # Wait for page to load
            except Exception as e:
                logger.error(f"Error navigating to {page_url}: {e}")
                    
                # Try once more
                try:
                    logger.info(f"Retrying page {page_num}")
                    await page.goto(page_url, {"waitUntil": "networkidle0", "timeout": 90000})
                    await asyncio.sleep(5)  # Wait longer for retry
                except Exception as e2:
                    logger.error(f"Error on retry: {e2}")
                    break
            
            # Extract products from the current page
            logger.info(f"Extracting products from page {page_num}")
            result = await scrape_products_from_page(page, category_name, total_products_count, len(all_products))
            
            if result == "NO_PRODUCTS":
                logger.info(f"Reached end of products at page {page_num}")
                break
                
            # Handle different result formats
            if isinstance(result, dict):
                page_products = result.get('products', [])
                section_info = result.get('section', 'Unknown section')
                logger.info(f"Section identified: {section_info}")
            elif isinstance(result, tuple) and len(result) >= 2:
                page_products = result[0]
                total_products_count = result[1]
            else:
                page_products = result
            
            if not page_products or len(page_products) == 0:
                logger.warning(f"No products found on page {page_num}")
                continue  # Try the next page instead of breaking
            
            logger.info(f"Found {len(page_products)} products on page {page_num}")
            
            # Check if any new products were added
            products_before = len(all_products)
            
            # Add page number to each product
            for product in page_products:
                product['page'] = page_num
            
            all_products.extend(page_products)
            products_after = len(all_products)
            new_products_added = products_after - products_before
            
            logger.info(f"Added {new_products_added} products from page {page_num} (total: {len(all_products)})")
            
            # Wait between pages to avoid being blocked
            if page_num < total_pages:
                await asyncio.sleep(2)
        
        logger.info(f"Completed scraping {total_pages} pages for {category_name}")
        logger.info(f"Total products found for {category_name}: {len(all_products)}")
        
        # Close the page
        await page.close()
        
        return all_products
        
    except Exception as e:
        logger.error(f"Error scraping category {category_name}: {e}")
        return []

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Coles Multi-Category Scraper')
    parser.add_argument('--max-pages', type=int, help='Maximum pages to scrape per category', default=None)
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode', default=True)
    parser.add_argument('--upload', action='store_true', help='Upload data to Supabase', default=False)
    parser.add_argument('--categories', type=str, help='Comma-separated list of categories to scrape (e.g. meat-seafood,bakery)', default=None)
    args = parser.parse_args()
    
    print("=" * 50)
    print("COLES MULTI-CATEGORY SCRAPER")
    print("=" * 50)
    
    # Create output directory for results
    output_dir = "coles_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a timestamp for this scraping run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create a summary file
    summary_filename = os.path.join(output_dir, f"summary_{timestamp}.txt")
    with open(summary_filename, 'w', encoding='utf-8') as f:
        f.write(f"Coles Scraping Run - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Categories to scrape: {len(CATEGORY_URLS)}\n\n")
    
    # Initialize the Coles product processor
    processor = ColesProductProcessor()
    
    # Initialize Supabase client if uploading
    supabase = None
    if args.upload:
        try:
            # Use environment variables for credentials
            supabase = SupabaseClient()
            logger.info("Initialized Supabase client")
        except Exception as e:
            logger.error(f"Error initializing Supabase: {e}")
            logger.warning("Continuing without Supabase upload capability")
    
    # Track overall statistics
    total_products = 0
    successful_categories = 0
    all_category_products = {}  # Store products for cross-category duplicate analysis
    
    # Filter categories if specified
    if args.categories:
        category_filters = args.categories.lower().split(',')
        filtered_urls = [url for url in CATEGORY_URLS if any(filter_name in url.lower() for filter_name in category_filters)]
        if filtered_urls:
            logger.info(f"Filtered to {len(filtered_urls)} categories: {args.categories}")
            category_urls = filtered_urls
        else:
            logger.warning(f"No categories matched filter: {args.categories}. Using all categories.")
            category_urls = CATEGORY_URLS
    else:
        category_urls = CATEGORY_URLS
    
    # Find Chrome or Edge path
    chrome_path = find_chrome_path()
    
    # Launch browser with specific executable path
    launch_options = {
        'headless': args.headless,
        'args': [
            '--no-sandbox', 
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--window-size=1920,1080'
        ],
    }
    
    if chrome_path:
        launch_options['executablePath'] = chrome_path
    
    browser = await launch(**launch_options)
    
    try:
        # Scrape each category
        for i, category_url in enumerate(category_urls, 1):
            category_match = re.search(r'browse/([^?]+)', category_url)
            category_name = category_match.group(1) if category_match else "unknown"
            
            print(f"\n[{i}/{len(category_urls)}] Scraping category: {category_name}")
            
            # Scrape products for this category
            products = await scrape_category(browser, category_url, args.max_pages)
            
            # Skip if no products found
            if not products:
                logger.warning(f"No products found for category: {category_name}")
                continue
            
            # Process products to ensure consistent format
            processed_products = processor.process_products(products)
            
            # Remove duplicates
            unique_products, duplicate_products = processor.remove_duplicates(processed_products)
            
            # Store for cross-category analysis
            all_category_products[category_name] = unique_products
            
            # Get duplicate report
            duplicate_report = processor.generate_duplicate_report(duplicate_products, category_name, timestamp)
            
            # Write duplicate report to file
            duplicate_report_file = os.path.join(output_dir, f"duplicates_{category_name}_{timestamp}.txt")
            with open(duplicate_report_file, 'w', encoding='utf-8') as f:
                f.write(duplicate_report)
            
            successful_categories += 1
            total_products += len(unique_products)
            
            # Get stats
            duplicates_removed = processor.product_stats.get('duplicates_removed', 0)
            expected_count = products[0].get('expected_count', 0) if products else 0
            
            # Create metadata
            metadata = {
                "category": category_name,
                "url": category_url,
                "timestamp": datetime.now().isoformat(),
                "expected_products": expected_count,
                "actual_products": len(unique_products),
                "duplicates_removed": duplicates_removed,
                "pages_scraped": max(product.get('page', 1) for product in unique_products) if unique_products else 0,
                "vendor": "coles"
            }
            
            # Create output data structure
            output_data = {
                "metadata": metadata,
                "products": unique_products
            }
            
            # Create filename with timestamp and category name
            filename = os.path.join(output_dir, f"coles_{category_name}_{timestamp}.json")
            
            # Save to JSON file
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2)
            
            print(f"Found {len(unique_products)} products for {category_name}")
            print(f"Removed {duplicates_removed} duplicates")
            print(f"Saved to {filename}")
            
            # Update summary file
            with open(summary_filename, 'a', encoding='utf-8') as f:
                f.write(f"{category_name}: {len(unique_products)} products")
                if expected_count:
                    f.write(f", expected: {expected_count} ({(len(unique_products)/expected_count*100):.1f}%)")
                f.write(f", duplicates removed: {duplicates_removed}")
                f.write("\n")
            
            # Upload to Supabase if enabled
            if args.upload and supabase:
                try:
                    # First, get existing product to determine if this is new or an update
                    existing_products = supabase.get_existing_product_ids(category_name)
                    new_products = [p for p in unique_products if p['id'] not in existing_products]
                    updated_products = [p for p in unique_products if p['id'] in existing_products]
                    
                    if new_products:
                        logger.info(f"Uploading {len(new_products)} new products to Supabase")
                        supabase.upload_products(new_products, category_name)
                    
                    if updated_products:
                        logger.info(f"Updating {len(updated_products)} existing products in Supabase")
                        supabase.upload_products(updated_products, category_name)
                    
                    # Upload category stats
                    supabase.upload_category_stats(category_name, metadata)
                    logger.info(f"Uploaded {len(new_products)} new and {len(updated_products)} updated products to Supabase")
                except Exception as e:
                    logger.error(f"Error uploading to Supabase: {e}")
            
            # Wait between categories to avoid being blocked
            if i < len(category_urls):
                wait_time = 5  # 5 seconds between categories
                print(f"Waiting {wait_time} seconds before next category...")
                await asyncio.sleep(wait_time)
    
    finally:
        await browser.close()
        logger.info("Browser closed")
    
    # Find cross-category duplicates
    cross_duplicates = processor.find_cross_category_duplicates(all_category_products)
    
    # Generate cross-category duplicate report
    cross_dup_report_file = os.path.join(output_dir, f"cross_category_duplicates_{timestamp}.txt")
    with open(cross_dup_report_file, 'w', encoding='utf-8') as f:
        f.write(f"Cross-Category Duplicates Report\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"Found {len(cross_duplicates)} products appearing in multiple categories\n\n")
        
        for i, (product_id, products) in enumerate(cross_duplicates.items(), 1):
            categories = set()
            titles = set()
            
            for category, product in products:
                categories.add(category)
                titles.add(product.get('title', 'Unknown'))
            
            # Use the first title found
            title = next(iter(titles)) if titles else 'Unknown'
            
            f.write(f"{i}. ID: {product_id} - {title}\n")
            f.write(f"   Found in {len(categories)} categories: {', '.join(sorted(categories))}\n\n")
    
    # Update summary with final statistics
    with open(summary_filename, 'a', encoding='utf-8') as f:
        f.write(f"\nSummary:\n")
        f.write(f"Total categories scraped successfully: {successful_categories}/{len(category_urls)}\n")
        f.write(f"Total products found: {total_products}\n")
        f.write(f"Products appearing in multiple categories: {len(cross_duplicates)}\n")
        f.write(f"Scraping completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Upload overall scraping run statistics to Supabase
    if args.upload and supabase:
        try:
            run_data = {
                'run_timestamp': datetime.strptime(timestamp, "%Y%m%d_%H%M%S").isoformat(),
                'categories_attempted': len(category_urls),
                'categories_successful': successful_categories,
                'total_products': total_products,
                'cross_category_duplicates': len(cross_duplicates),
                'vendor': 'coles'
            }
            supabase.upload_scraping_run(run_data)
        except Exception as e:
            logger.error(f"Error uploading scraping run statistics: {e}")
    
    print("\n" + "=" * 50)
    print(f"SCRAPING COMPLETE")
    print(f"Total categories: {len(category_urls)}")
    print(f"Successful categories: {successful_categories}")
    print(f"Total products found: {total_products}")
    print(f"Products appearing in multiple categories: {len(cross_duplicates)}")
    print(f"Summary saved to: {summary_filename}")
    print("=" * 50)

if __name__ == "__main__":
    asyncio.run(main())