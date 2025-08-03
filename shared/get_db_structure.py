import os
import json
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

def get_database_structure():
    """Fetch the database structure from Supabase"""
    url = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    
    if not url or not key:
        raise ValueError("Supabase credentials not found in environment variables")
    
    client = create_client(url, key)
    
    # Query to get table information
    tables_query = """
    SELECT 
        table_name,
        table_type
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
    """
    
    # Query to get column information
    columns_query = """
    SELECT 
        table_name,
        column_name,
        data_type,
        is_nullable,
        column_default,
        character_maximum_length
    FROM information_schema.columns
    WHERE table_schema = 'public'
    ORDER BY table_name, ordinal_position;
    """
    
    # Query to get constraints
    constraints_query = """
    SELECT
        tc.table_name,
        tc.constraint_name,
        tc.constraint_type,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    LEFT JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
    WHERE tc.table_schema = 'public'
    ORDER BY tc.table_name, tc.constraint_type;
    """
    
    try:
        # Execute queries
        tables_result = client.rpc('sql', {'query': tables_query}).execute()
        columns_result = client.rpc('sql', {'query': columns_query}).execute()
        constraints_result = client.rpc('sql', {'query': constraints_query}).execute()
        
        return {
            'tables': tables_result.data if hasattr(tables_result, 'data') else [],
            'columns': columns_result.data if hasattr(columns_result, 'data') else [],
            'constraints': constraints_result.data if hasattr(constraints_result, 'data') else []
        }
    except Exception as e:
        print(f"Error fetching database structure: {e}")
        # Try alternative approach using direct table queries
        return fetch_structure_alternative(client)

def fetch_structure_alternative(client):
    """Alternative method to fetch structure by querying known tables"""
    structure = {'tables': [], 'columns': [], 'constraints': []}
    
    # Known tables based on the code
    known_tables = ['products', 'category_stats', 'scraping_runs']
    
    for table in known_tables:
        try:
            # Get a sample row to infer structure
            result = client.table(table).select('*').limit(1).execute()
            
            if hasattr(result, 'data') and result.data:
                structure['tables'].append({
                    'table_name': table,
                    'table_type': 'BASE TABLE'
                })
                
                # Infer columns from the sample data
                sample = result.data[0]
                for column_name, value in sample.items():
                    data_type = 'text'
                    if isinstance(value, bool):
                        data_type = 'boolean'
                    elif isinstance(value, int):
                        data_type = 'integer'
                    elif isinstance(value, float):
                        data_type = 'numeric'
                    elif isinstance(value, dict):
                        data_type = 'jsonb'
                    
                    structure['columns'].append({
                        'table_name': table,
                        'column_name': column_name,
                        'data_type': data_type,
                        'is_nullable': 'YES',
                        'column_default': None,
                        'character_maximum_length': None
                    })
            else:
                # Table exists but is empty, add basic info
                structure['tables'].append({
                    'table_name': table,
                    'table_type': 'BASE TABLE'
                })
                
        except Exception as e:
            print(f"Could not fetch structure for table {table}: {e}")
    
    return structure

def generate_sql_files(structure):
    """Generate SQL files from the database structure"""
    os.makedirs('sql', exist_ok=True)
    
    # Group columns by table
    tables_info = {}
    for col in structure['columns']:
        table_name = col['table_name']
        if table_name not in tables_info:
            tables_info[table_name] = []
        tables_info[table_name].append(col)
    
    # Group constraints by table
    constraints_info = {}
    for constraint in structure['constraints']:
        table_name = constraint['table_name']
        if table_name not in constraints_info:
            constraints_info[table_name] = []
        constraints_info[table_name].append(constraint)
    
    # Generate CREATE TABLE statements
    all_tables_sql = []
    
    for table in structure['tables']:
        table_name = table['table_name']
        if table_name not in tables_info:
            continue
            
        sql = f"CREATE TABLE {table_name} (\n"
        
        # Add columns
        columns = tables_info[table_name]
        column_defs = []
        for col in columns:
            col_def = f"    {col['column_name']} {col['data_type']}"
            
            if col['character_maximum_length']:
                col_def += f"({col['character_maximum_length']})"
            
            if col['is_nullable'] == 'NO':
                col_def += " NOT NULL"
            
            if col['column_default']:
                col_def += f" DEFAULT {col['column_default']}"
            
            column_defs.append(col_def)
        
        sql += ",\n".join(column_defs)
        
        # Add constraints
        if table_name in constraints_info:
            for constraint in constraints_info[table_name]:
                if constraint['constraint_type'] == 'PRIMARY KEY':
                    sql += f",\n    CONSTRAINT {constraint['constraint_name']} PRIMARY KEY ({constraint['column_name']})"
                elif constraint['constraint_type'] == 'FOREIGN KEY':
                    sql += f",\n    CONSTRAINT {constraint['constraint_name']} FOREIGN KEY ({constraint['column_name']}) REFERENCES {constraint['foreign_table_name']}({constraint['foreign_column_name']})"
                elif constraint['constraint_type'] == 'UNIQUE':
                    sql += f",\n    CONSTRAINT {constraint['constraint_name']} UNIQUE ({constraint['column_name']})"
        
        sql += "\n);\n"
        
        # Save individual table file
        with open(f'sql/{table_name}.sql', 'w') as f:
            f.write(sql)
        
        all_tables_sql.append(sql)
    
    # Save all tables in one file
    with open('sql/all_tables.sql', 'w') as f:
        f.write('\n\n'.join(all_tables_sql))
    
    # Save structure as JSON for reference
    with open('sql/db_structure.json', 'w') as f:
        json.dump(structure, f, indent=2)
    
    print(f"Generated SQL files for {len(tables_info)} tables")
    return tables_info

if __name__ == "__main__":
    print("Fetching database structure from Supabase...")
    structure = get_database_structure()
    
    if structure['tables']:
        print(f"Found {len(structure['tables'])} tables")
        tables_info = generate_sql_files(structure)
        
        print("\nDatabase Tables:")
        for table_name, columns in tables_info.items():
            print(f"\n{table_name}:")
            for col in columns:
                nullable = "" if col['is_nullable'] == 'NO' else " (nullable)"
                print(f"  - {col['column_name']}: {col['data_type']}{nullable}")
    else:
        print("No tables found or could not connect to database")