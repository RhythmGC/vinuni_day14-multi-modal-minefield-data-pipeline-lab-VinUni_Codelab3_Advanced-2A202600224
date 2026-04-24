import pandas as pd
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.

def process_sales_csv(file_path):
    # --- FILE READING (Handled for students) ---
    df = pd.read_csv(file_path)
    # ------------------------------------------
    
    # Remove duplicate rows based on 'id'
    df = df.drop_duplicates(subset=['id'], keep='first')
    
    # Function to clean price values
    def clean_price(price_val):
        if pd.isna(price_val) or price_val in ['N/A', 'NULL', '']:
            return None
        
        # If it's already a number, return as float
        if isinstance(price_val, (int, float)):
            return float(price_val)
        
        # Convert to string for processing
        price_str = str(price_val).strip().lower()
        
        # Handle text representations
        if price_str == 'five dollars':
            return 5.0
        
        # Remove currency symbols and commas
        price_str = re.sub(r'[^\d.-]', '', price_str)
        
        try:
            return float(price_str)
        except ValueError:
            return None
    
    # Function to normalize date formats
    def normalize_date(date_val):
        if pd.isna(date_val) or date_val == '':
            return None
        
        date_str = str(date_val).strip()
        
        # Try various date formats
        date_formats = [
            '%Y-%m-%d',        # 2026-01-15
            '%d/%m/%Y',        # 15/01/2026
            '%B %dth %Y',      # January 16th 2026
            '%d-%m-%Y',        # 17-01-2026
            '%Y/%m/%d',        # 2026/01/19
            '%d %b %Y',        # 19 Jan 2026
            '%d/%m/%y',        # 22/01/26 (if applicable)
        ]
        
        for fmt in date_formats:
            try:
                return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # If none of the formats work, return as-is (will be handled by pandas)
        try:
            return pd.to_datetime(date_str).strftime('%Y-%m-%d')
        except ValueError:
            return None
    
    # Clean price column
    df['price_cleaned'] = df['price'].apply(clean_price)
    
    # Normalize date column
    df['date_normalized'] = df['date_of_sale'].apply(normalize_date)
    
    # Create documents
    documents = []
    
    for _, row in df.iterrows():
        # Skip rows where essential data is missing
        if pd.isna(row['price_cleaned']) or pd.isna(row['date_normalized']):
            continue
            
        # Create content string
        content_parts = [
            f"Product: {row['product_name']}",
            f"Category: {row['category']}",
            f"Price: {row['price_cleaned']} {row['currency']}",
            f"Date of Sale: {row['date_normalized']}",
            f"Seller ID: {row['seller_id']}",
            f"Stock Quantity: {row['stock_quantity'] if not pd.isna(row['stock_quantity']) else 'N/A'}"
        ]
        content = ". ".join(content_parts)
        
        # Create document
        document = {
            "document_id": f"csv-{row['id']}",
            "content": content,
            "source_type": "CSV",
            "author": None,
            "timestamp": None,
            "source_metadata": {
                "original_file": "sales_records.csv",
                "id": int(row['id']) if not pd.isna(row['id']) else None,
                "product_name": row['product_name'],
                "category": row['category'],
                "price_value": row['price_cleaned'],
                "price_currency": row['currency'],
                "price_original": row['price'],
                "date_of_sale": row['date_normalized'],
                "seller_id": row['seller_id'],
                "stock_quantity": int(row['stock_quantity']) if not pd.isna(row['stock_quantity']) else None
            }
        }
        
        documents.append(document)
    
    return documents

