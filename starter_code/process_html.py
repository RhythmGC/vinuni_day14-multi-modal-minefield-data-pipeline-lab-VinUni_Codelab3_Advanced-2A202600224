from bs4 import BeautifulSoup
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.

def parse_html_catalog(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
    # ------------------------------------------
    
    # Use BeautifulSoup to find the table with id 'main-catalog'
    table = soup.find('table', id='main-catalog')
    if not table:
        return []
    
    documents = []
    
    # Extract rows from the table body
    tbody = table.find('tbody')
    if not tbody:
        return []
    
    rows = tbody.find_all('tr')
    
    for i, row in enumerate(rows):
        cols = row.find_all('td')
        if len(cols) >= 6:  # We need at least 6 columns
            # Extract text from each column
            product_id = cols[0].get_text(strip=True)
            product_name = cols[1].get_text(strip=True)
            category = cols[2].get_text(strip=True)
            price_text = cols[3].get_text(strip=True)
            stock_text = cols[4].get_text(strip=True)
            rating_text = cols[5].get_text(strip=True)
            
            # Process price - handle 'N/A' or 'Liên hệ'
            price_value = None
            currency = None
            
            if price_text and price_text not in ['N/A', 'Liên hệ']:
                # Extract numeric value and currency
                # Examples: "28,500,000 VND" or "1,850,000 VND"
                price_match = re.search(r'([\d,\s]+)\s*(\w+)', price_text)
                if price_match:
                    price_str = price_match.group(1).replace(',', '').replace(' ', '')
                    try:
                        price_value = float(price_str)
                        currency = price_match.group(2)
                    except ValueError:
                        pass
            
            # Process stock - handle negative values or non-numeric
            stock_value = None
            try:
                stock_value = int(stock_text) if stock_text and stock_text.strip() != '' else None
            except ValueError:
                pass
            
            # Process rating
            rating_value = None
            if rating_text and rating_text != 'không có đánh giá':
                try:
                    # Extract first number from rating like "4.8/5"
                    rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                    if rating_match:
                        rating_value = float(rating_match.group(1))
                except ValueError:
                    pass
            
            # Create content string
            content_parts = [
                f"Product ID: {product_id}",
                f"Product Name: {product_name}",
                f"Category: {category}",
                f"Price: {price_text if price_text else 'N/A'}",
                f"Stock: {stock_text if stock_text else 'N/A'}",
                f"Rating: {rating_text if rating_text else 'N/A'}"
            ]
            content = ". ".join(content_parts)
            
            # Create document
            document = {
                "document_id": f"html-{product_id}",
                "content": content,
                "source_type": "HTML",
                "author": None,
                "timestamp": None,
                "source_metadata": {
                    "original_file": "product_catalog.html",
                    "product_id": product_id,
                    "product_name": product_name,
                    "category": category,
                    "price_value": price_value,
                    "price_currency": currency,
                    "price_raw": price_text,
                    "stock_value": stock_value,
                    "stock_raw": stock_text,
                    "rating_value": rating_value,
                    "rating_raw": rating_text
                }
            }
            
            documents.append(document)
    
    return documents

