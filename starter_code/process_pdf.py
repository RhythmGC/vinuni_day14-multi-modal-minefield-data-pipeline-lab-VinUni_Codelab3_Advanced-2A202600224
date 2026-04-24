import google.generativeai as genai
import os
import json
import time
from dotenv import load_dotenv

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

def extract_pdf_data(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None
        
    # Use the correct model name
    model = genai.GenerativeModel('gemini-1.5-flash')
    
    print(f"Uploading {file_path} to Gemini...")
    try:
        pdf_file = genai.upload_file(path=file_path)
    except Exception as e:
        print(f"Failed to upload file to Gemini: {e}")
        return None
        
    prompt = """
Analyze this document and extract the title, author, main topics, and any tables.
Output exactly as a JSON object matching this exact format:
{
    "document_id": "pdf-doc-001",
    "content": "Title: [Title]. Author: [Author]. Main Topics: [Main Topics]. Tables: [Description of tables found].",
    "source_type": "PDF",
    "author": "[Author name]",
    "timestamp": null,
    "source_metadata": {
        "original_file": "lecture_notes.pdf",
        "main_topics": ["topic1", "topic2", "topic3"],
        "tables_found": true/false
    }
}
    """
    
    # Implement exponential backoff for 429 errors
    max_retries = 5
    base_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            print(f"Generating content from PDF using Gemini (attempt {attempt + 1}/{max_retries})...")
            response = model.generate_content([pdf_file, prompt])
            content_text = response.text
            
            # Simple cleanup if the response is wrapped in markdown json block
            if content_text.startswith("```json"):
                content_text = content_text[7:]
            if content_text.endswith("```"):
                content_text = content_text[:-3]
            if content_text.startswith("```"):
                content_text = content_text[3:]
                
            extracted_data = json.loads(content_text.strip())
            return extracted_data
        except Exception as e:
            if "429" in str(e) and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                print(f"Rate limit exceeded. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"Failed to generate content from PDF: {e}")
                return None
    
    return None
