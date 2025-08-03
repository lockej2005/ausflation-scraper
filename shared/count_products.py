import asyncio
import logging
import os
import json
from datetime import datetime
from pyppeteer import launch

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("WoolworthsCounter")

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

async def scrape_products(url, headless=True):
    """Scrape products from a Woolworths category page"""
    logger.info(f"Opening page: {url}")
    
    # Find Chrome or Edge path
    chrome_path = find_chrome_path()
    
    # Launch browser with specific executable path
    launch_options = {
        'headless': headless,
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
        # Open new page
        page = await browser.newPage()
        
        # Set a realistic user agent
        await page.setUserAgent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
        
        # Navigate to the category page
        logger.info("Loading page...")
        await page.goto(url, {"waitUntil": "networkidle0", "timeout": 60000})
        
        # Wait a bit for any dynamic content
        await asyncio.sleep(5)
        
        # Extract products with titles and prices based on the exact HTML structure you provided
        products = await page.evaluate('''
            () => {
                const products = [];
                
                // Find all product tiles
                const productElements = document.querySelectorAll('wc-product-tile');
                
                console.log(`Found ${productElements.length} product elements`);
                
                // Process each product element
                productElements.forEach((element, index) => {
                    try {
                        // Access the shadow DOM
                        const shadowRoot = element.shadowRoot;
                        
                        if (!shadowRoot) {
                            console.log(`No shadow root for element ${index}`);
                            products.push({
                                title: `Product ${index + 1}`,
                                price: "Price unavailable",
                                unit_price: ""
                            });
                            return;
                        }
                        
                        // Look for title - based on your HTML it's in a div with class "title" containing an <a> tag
                        let title = "Unknown Product";
                        const titleElement = shadowRoot.querySelector('.title a');
                        if (titleElement) {
                            title = titleElement.textContent.trim();
                        }
                        
                        // Look for price - based on your HTML it's in a div with class "primary"
                        let price = "Price unavailable";
                        const priceElement = shadowRoot.querySelector('.primary');
                        if (priceElement) {
                            price = priceElement.textContent.trim();
                        }
                        
                        // Look for unit price
                        let unitPrice = "";
                        const unitPriceElement = shadowRoot.querySelector('.price-per-cup');
                        if (unitPriceElement) {
                            unitPrice = unitPriceElement.textContent.trim();
                        }
                        
                        // Get the product URL if available
                        let productUrl = "";
                        const linkElement = shadowRoot.querySelector('.title a');
                        if (linkElement && linkElement.href) {
                            productUrl = linkElement.href;
                        }
                        
                        // Add to products array
                        products.push({
                            title: title,
                            price: price,
                            unit_price: unitPrice,
                            url: productUrl
                        });
                    } catch (error) {
                        console.error(`Error processing product ${index}:`, error);
                        products.push({
                            title: `Product ${index + 1}`,
                            price: "Error extracting data",
                            unit_price: ""
                        });
                    }
                });
                
                return products;
            }
        ''')
        
        # Count the products
        count = len(products)
        logger.info(f"Found {count} products on the page")
        
        return products
        
    finally:
        await browser.close()
        logger.info("Browser closed")

async def main():
    # Woolworths category URL
    category_url = "https://www.woolworths.com.au/shop/browse/fruit-veg"
    
    print("=" * 50)
    print("WOOLWORTHS PRODUCT SCRAPER")
    print("=" * 50)
    
    # Scrape products (with headless=True to run invisibly)
    products = await scrape_products(category_url, headless=True)
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"woolworths_products_{timestamp}.json"
    
    # Save to JSON file
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(products, f, indent=2)
    
    print(f"\nFound a total of {len(products)} products")
    print(f"Saved product data to {filename}")
    print("=" * 50)
    
    # Print the first 5 products as a sample
    if products:
        print("\nSample of products:")
        for i, product in enumerate(products[:5], 1):
            print(f"{i}. {product.get('title')} - {product.get('price')} ({product.get('unit_price')})")

if __name__ == "__main__":
    asyncio.run(main())