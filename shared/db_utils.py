import os
import json
import logging
from datetime import datetime
from supabase import create_client

logger = logging.getLogger("DBUtils")

class SupabaseClient:
    """Handles all interactions with the Supabase database"""
    
    def __init__(self, url=None, key=None):
        """Initialize the Supabase client with credentials"""
        self.url = url or os.environ.get("SUPABASE_URL")
        self.key = key or os.environ.get("SUPABASE_KEY")
        
        if not self.url or not self.key:
            raise ValueError("Supabase URL and key must be provided via parameters or environment variables")
        
        self.client = create_client(self.url, self.key)
        logger.info("Supabase client initialized")
    
    def upload_products(self, products, category):
        """Upload products to the Supabase database"""
        if not products:
            logger.warning(f"No products to upload for category {category}")
            return 0
            
        # Prepare products for insertion
        for product in products:
            # Add timestamp
            product['uploaded_at'] = datetime.now().isoformat()
            # Ensure category is set
            product['category'] = category
            
            # Make sure the field is named scrapedAt, not scraped_at
            if 'scraped_at' in product:
                product['scrapedAt'] = product.pop('scraped_at')
        
        # Insert in batches to avoid request size limitations
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(products), batch_size):
            batch = products[i:i+batch_size]
            try:
                result = self.client.table('products').upsert(
                    batch, 
                    on_conflict='id'
                ).execute()
                
                # Check for errors
                if hasattr(result, 'error') and result.error:
                    logger.error(f"Error inserting batch: {result.error}")
                else:
                    # Count successful insertions
                    if hasattr(result, 'data'):
                        total_inserted += len(result.data)
                    else:
                        total_inserted += len(batch)
                        
                logger.info(f"Inserted batch of {len(batch)} products")
            except Exception as e:
                logger.error(f"Error uploading batch to Supabase: {e}")
        
        logger.info(f"Uploaded {total_inserted} products for category {category}")
        return total_inserted
    
    def upload_category_stats(self, category, stats):
        """Upload category statistics to the Supabase database"""
        try:
            # Prepare data with proper column names
            upload_data = {
                'category': category,
                'expected_products': stats.get('expected_products', 0),
                'actual_products': stats.get('actual_products', 0),
                'duplicates_removed': stats.get('duplicates_removed', 0),
                'pages_scraped': stats.get('pages_scraped', 0),
                'updated_at': datetime.now().isoformat()
            }
            
            result = self.client.table('category_stats').upsert(
                upload_data,
                on_conflict='category'
            ).execute()
            
            # Check for errors
            if hasattr(result, 'error') and result.error:
                logger.error(f"Error inserting category stats: {result.error}")
                return False
            return True
        except Exception as e:
            logger.error(f"Error uploading category stats to Supabase: {e}")
            return False
    
    def upload_scraping_run(self, run_data):
        """Upload information about the scraping run"""
        try:
            # Ensure proper field names to match the table schema
            upload_data = {
                'run_timestamp': run_data.get('run_timestamp', datetime.now().isoformat()),
                'categories_attempted': run_data.get('categories_attempted', 0),
                'categories_successful': run_data.get('categories_successful', 0),
                'total_products': run_data.get('total_products', 0),
                'cross_category_duplicates': run_data.get('cross_category_duplicates', 0),
                'completed_at': datetime.now().isoformat()
            }
            
            result = self.client.table('scraping_runs').insert(upload_data).execute()
            
            # Check for errors
            if hasattr(result, 'error') and result.error:
                logger.error(f"Error inserting scraping run: {result.error}")
                return False
                
            logger.info("Successfully recorded scraping run in database")
            return True
        except Exception as e:
            logger.error(f"Error uploading scraping run to Supabase: {e}")
            return False
            
    def get_existing_product_ids(self, category=None):
        """Get a set of existing product IDs, optionally filtered by category"""
        try:
            query = self.client.table('products').select('id')
            
            if category:
                query = query.eq('category', category)
                
            result = query.execute()
            
            if hasattr(result, 'data'):
                return {item['id'] for item in result.data if 'id' in item}
            return set()
        except Exception as e:
            logger.error(f"Error fetching existing products: {e}")
            return set()