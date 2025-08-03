import logging
import re
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger("ColesProductProcessor")

class ColesProductProcessor:
    """Handles processing and deduplication of Coles products"""
    
    def __init__(self):
        """Initialize the product processor"""
        self.product_stats = {}
    
    def extract_price_value(self, price_string):
        """Extract numeric price value from a string, returning a float without $ sign"""
        if not price_string:
            return 0.0
            
        # Remove $ and any other non-numeric characters except decimal point
        price_string = price_string.replace('$', '')
        try:
            # Try to find numbers in the string
            match = re.search(r'(\d+\.\d+|\d+)', price_string)
            if match:
                return float(match.group(1))
        except (ValueError, AttributeError):
            pass
            
        return 0.0
    
    def process_products(self, products):
        """Process and clean product data specifically for Coles products"""
        processed_products = []
        
        for product in products:
            # Skip products without important fields
            if not product.get('title'):
                continue
                
            # If ID is missing but URL is present, try to extract ID from URL
            if not product.get('id') and product.get('url'):
                url = product.get('url', '')
                id_match = re.search(r'-(\d+)$', url)
                if id_match:
                    product['id'] = id_match.group(1)
            
            # Standardize vendor field
            product['vendor'] = 'coles'
            
            # Clean up unit price
            unit_price = product.get('unit_price', '')
            if unit_price:
                # Remove any "Was" price that might be included
                unit_price = unit_price.split('|')[0].strip()
                product['unit_price'] = unit_price
            
            # Extract per kg/per 100g rate from the unit price if available
            if unit_price:
                try:
                    # Extract rate like $X.XX per kg or $X.XX per 100g
                    rate_match = re.search(r'\$([\d\.]+)\s+per\s+(\w+)', unit_price)
                    if rate_match:
                        rate_value = float(rate_match.group(1))
                        rate_unit = rate_match.group(2).lower()
                        product['rate_value'] = rate_value
                        product['rate_unit'] = rate_unit
                except Exception as e:
                    logger.warning(f"Error extracting unit price rate: {e}")
            
            # Parse special text
            special_text = product.get('special_text', '')
            if special_text:
                product['is_on_special'] = True
                product['special_text'] = special_text.strip()
            else:
                product['is_on_special'] = False
            
            # Ensure save_value is properly calculated
            was_price = product.get('was_price_value', 0.0)
            current_price = product.get('price_value', 0.0)
            save_value = product.get('save_value', 0.0)
            
            # If save_value is 0 but we have was_price, calculate savings
            if save_value == 0 and was_price > 0 and current_price > 0:
                product['save_value'] = round(was_price - current_price, 2)
            
            # Create a cleaned product object
            processed_product = {
                'id': product.get('id', ''),
                'title': product.get('title', '').strip(),
                'price_value': product.get('price_value', 0.0),
                'was_price_value': product.get('was_price_value', 0.0),
                'save_value': product.get('save_value', 0.0),
                'unit_price': product.get('unit_price', ''),
                'url': product.get('url', ''),
                'image_url': product.get('image_url', ''),
                'category': product.get('category', ''),
                'page': product.get('page', 0),
                'vendor': 'coles',
                'is_on_special': product.get('is_on_special', False),
                'special_text': product.get('special_text', ''),
                'rate_value': product.get('rate_value', 0.0),
                'rate_unit': product.get('rate_unit', ''),
                'scrapedAt': product.get('scrapedAt', datetime.now().isoformat())
            }
            
            # Remove any None values
            processed_product = {k: v for k, v in processed_product.items() if v is not None}
            
            processed_products.append(processed_product)
            
        return processed_products
    
    def remove_duplicates(self, products):
        """Remove duplicate products based on ID"""
        unique_products = []
        seen_ids = set()
        duplicate_products = []
        
        for product in products:
            product_id = product.get('id', '')
            if product_id and product_id not in seen_ids:
                seen_ids.add(product_id)
                unique_products.append(product)
            elif product_id:  # This is a duplicate
                duplicate_products.append(product)
            elif not product_id:  # Keep products without IDs (rare case)
                # For products without IDs, use title + price as a pseudo-id
                pseudo_id = f"{product.get('title', '')}-{product.get('price_value', 0)}"
                if pseudo_id not in seen_ids:
                    seen_ids.add(pseudo_id)
                    unique_products.append(product)
                else:
                    duplicate_products.append(product)
        
        duplicates_removed = len(products) - len(unique_products)
        
        self.product_stats = {
            'total_before': len(products),
            'total_after': len(unique_products),
            'duplicates_removed': duplicates_removed
        }
        
        logger.info(f"Removed {duplicates_removed} duplicate products")
        return unique_products, duplicate_products
    
    def generate_duplicate_report(self, duplicate_products, category_name, timestamp):
        """Generate a report of duplicate products"""
        report_lines = [
            f"Duplicate Removal Report for Coles {category_name}",
            f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            f"Total products before removal: {self.product_stats.get('total_before', 0)}",
            f"Unique products after removal: {self.product_stats.get('total_after', 0)}",
            f"Duplicates removed: {self.product_stats.get('duplicates_removed', 0)}",
            ""
        ]
        
        if duplicate_products:
            report_lines.append("Removed duplicate products:")
            for i, product in enumerate(duplicate_products, 1):
                report_lines.append(f"{i}. ID: {product.get('id', 'Unknown')} - {product.get('title', 'Unknown')}")
                report_lines.append(f"   Price: ${product.get('price_value', 0)}, Category: {product.get('category', 'N/A')}")
                if product.get('is_on_special'):
                    report_lines.append(f"   Special: {product.get('special_text', 'Yes')}")
                report_lines.append("")
        
        return '\n'.join(report_lines)
    
    def find_cross_category_duplicates(self, all_category_products):
        """Find products that appear in multiple categories"""
        products_by_id = defaultdict(list)
        
        # Group products by ID
        for category, products in all_category_products.items():
            for product in products:
                product_id = product.get('id')
                if product_id:
                    products_by_id[product_id].append((category, product))
        
        # Find products that appear in multiple categories
        cross_duplicates = {
            product_id: products 
            for product_id, products in products_by_id.items() 
            if len(set(category for category, _ in products)) > 1
        }
        
        return cross_duplicates