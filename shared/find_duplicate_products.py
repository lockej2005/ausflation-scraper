import os
import json
import glob
from collections import defaultdict
import pandas as pd

def find_duplicates():
    """
    Find duplicate products across all JSON files in the woolworths_data directory
    using product ID as the unique identifier.
    """
    print("Woolworths Duplicate Product Finder")
    print("=" * 50)
    
    # Find all JSON files in the woolworths_data directory
    json_files = glob.glob(os.path.join('woolworths_data', '*.json'))
    print(f"Found {len(json_files)} JSON files to analyze")
    
    # Dictionary to store products by ID
    products_by_id = defaultdict(list)
    
    # Dictionary to store counts and stats by category
    category_stats = defaultdict(dict)
    
    # Track total products processed
    total_products = 0
    total_duplicates_removed = 0
    
    # Process each JSON file
    for json_file in json_files:
        category = os.path.basename(json_file).split('_')[1]  # Extract category from filename
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Handle both new format (with metadata and products) and old format (just products)
                products = data.get('products', data)
                
                # Extract metadata if available
                metadata = data.get('metadata', {})
                expected_products = data.get('expected_products', metadata.get('total_products_reported', 0))
                actual_products = data.get('actual_products', len(products))
                
                if not isinstance(products, list):
                    print(f"Warning: Unexpected data format in {json_file}")
                    continue
                
                # Count products in this category
                total_products += len(products)
                total_duplicates_removed += duplicates_removed
                
                # Process each product
                for product in products:
                    product_id = product.get('id')
                    
                    # Skip products without an ID
                    if not product_id:
                        continue
                    
                    # Add file information to help identify source
                    product['source_file'] = os.path.basename(json_file)
                    
                    # Add to our ID tracking dictionary
                    products_by_id[product_id].append(product)
                    
                # Add category stats including duplicate information
                duplicates_removed = data.get('duplicates_removed', 0)
                category_stats[category] = {
                    'count': len(products),
                    'expected': expected_products,
                    'duplicates_removed': duplicates_removed,
                    'source_file': os.path.basename(json_file)
                }
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
    
    # Find products with duplicate IDs
    duplicates = {id: products for id, products in products_by_id.items() 
                 if len(products) > 1 and id}  # Exclude empty IDs
    
    # Create a list to store category stats
    category_summary = []
    total_expected = 0
    total_actual = 0
    
    # Collect stats for each category
    for category, stats in category_stats.items():
        count = stats.get('count', 0)
        expected = stats.get('expected', 0)
        duplicates = stats.get('duplicates_removed', 0)
        
        if expected > 0:
            total_expected += expected
        
        total_actual += count
        category_summary.append((category, count, expected, duplicates))
    
    # Print summary
    print("\nSummary:")
    print(f"Total products processed: {total_products}")
    print(f"Unique product IDs: {len(products_by_id)}")
    print(f"Products with duplicate IDs across categories: {len(duplicates)}")
    print(f"Total duplicates removed during scraping: {total_duplicates_removed}")
    
    # Create global duplicates report
    global_duplicates_report = 'global_duplicates_report.txt'
    with open(global_duplicates_report, 'w', encoding='utf-8') as f:
        f.write("Woolworths Global Duplicates Report\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"Total products analyzed: {total_products}\n")
        f.write(f"Unique product IDs: {len(products_by_id)}\n")
        f.write(f"Products appearing in multiple categories: {len(duplicates)}\n")
        f.write(f"Duplicates removed during initial scraping: {total_duplicates_removed}\n\n")
        
        f.write("Category Summary:\n")
        for category, count, expected, duplicates in sorted(category_summary, key=lambda x: x[0]):
            f.write(f"  {category}: {count} products")
            if expected > 0:
                coverage = (count / expected) * 100
                f.write(f", {coverage:.1f}% of expected {expected}")
            f.write(f", {duplicates} duplicates removed\n")
            
        if duplicates:
            f.write("\n\nProducts found in multiple categories:\n")
            for i, (product_id, products) in enumerate(duplicates.items(), 1):
                categories = sorted(set(p.get('category', 'unknown') for p in products))
                f.write(f"{i}. ID: {product_id} - {products[0].get('title', 'Unknown')}\n")
                f.write(f"   Found in {len(categories)} categories: {', '.join(categories)}\n")
    
    print(f"\nGlobal duplicates report saved to {global_duplicates_report}")
    
    if total_expected > 0:
        coverage = (total_actual / total_expected) * 100
        print(f"Overall coverage: {total_actual}/{total_expected} ({coverage:.1f}%)")
    
    # Print category breakdown with expected counts
    print("\nProducts per category:")
    for category, count, expected, duplicates in sorted(category_summary, key=lambda x: x[1], reverse=True):
        output = f"  {category}: {count}"
        if expected > 0:
            coverage = (count / expected) * 100
            output += f"/{expected} ({coverage:.1f}%)"
        if duplicates > 0:
            output += f", {duplicates} duplicates removed"
        print(output)
    
    # Write duplicates to CSV for further analysis
    if duplicates:
        # Prepare data for CSV
        csv_data = []
        
        for product_id, products in duplicates.items():
            category_list = [p.get('category', 'unknown') for p in products]
            category_str = ', '.join(set(category_list))
            
            for product in products:
                csv_data.append({
                    'id': product_id,
                    'title': product.get('title', 'Unknown'),
                    'price': product.get('price', 'N/A'),
                    'category': product.get('category', 'N/A'),
                    'all_categories': category_str,
                    'source_file': product.get('source_file', 'N/A'),
                    'duplicate_count': len(products),
                    'url': product.get('url', 'N/A'),
                })
        
        # Convert to DataFrame and save
        df = pd.DataFrame(csv_data)
        output_file = 'woolworths_duplicate_products.csv'
        df.to_csv(output_file, index=False)
        print(f"\nSaved {len(csv_data)} duplicate products to {output_file}")
        
        # Print some examples
        print("\nExamples of duplicates:")
        sample_ids = list(duplicates.keys())[:5]  # Get first 5 duplicate IDs
        
        for i, product_id in enumerate(sample_ids, 1):
            products = duplicates[product_id]
            categories = ', '.join(set(p.get('category', 'unknown') for p in products))
            print(f"\n{i}. ID: {product_id}")
            print(f"   Title: {products[0].get('title', 'Unknown')}")
            print(f"   Found in {len(products)} categories: {categories}")
    else:
        print("\nNo duplicates found!")
    
    # Find products with same title but different IDs (possible duplicates with different SKUs)
    print("\nChecking for products with same title but different IDs...")
    products_by_title = defaultdict(list)
    
    # Group products by title
    for product_id, products in products_by_id.items():
        for product in products:
            title = product.get('title')
            if title:  # Skip products without a title
                products_by_title[title].append(product)
    
    # Find titles with multiple products
    title_duplicates = {title: products for title, products in products_by_title.items() 
                       if len(set(p.get('id') for p in products)) > 1}  # Different IDs
    
    print(f"Found {len(title_duplicates)} product titles with different IDs")
    
    if title_duplicates:
        # Save to CSV
        csv_data = []
        
        for title, products in title_duplicates.items():
            product_ids = list(set(p.get('id') for p in products))
            
            for product in products:
                csv_data.append({
                    'title': title,
                    'id': product.get('id', 'N/A'),
                    'price': product.get('price', 'N/A'),
                    'category': product.get('category', 'N/A'),
                    'all_ids': ', '.join(product_ids),
                    'id_count': len(product_ids),
                    'source_file': product.get('source_file', 'N/A'),
                })
        
        # Convert to DataFrame and save
        df = pd.DataFrame(csv_data)
        output_file = 'woolworths_title_duplicates.csv'
        df.to_csv(output_file, index=False)
        print(f"Saved {len(csv_data)} products with duplicate titles to {output_file}")
        
        # Print some examples
        print("\nExamples of same title with different IDs:")
        sample_titles = list(title_duplicates.keys())[:5]  # Get first 5 titles
        
        for i, title in enumerate(sample_titles, 1):
            products = title_duplicates[title]
            ids = ', '.join(set(p.get('id', 'N/A') for p in products))
            print(f"\n{i}. Title: {title}")
            print(f"   IDs: {ids}")
            print(f"   Categories: {', '.join(set(p.get('category', 'unknown') for p in products))}")

if __name__ == "__main__":
    find_duplicates()