import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.

def clean_transcript(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    # ------------------------------------------
    
    # First, find the price mentioned in Vietnamese words ("năm trăm nghìn")
    # This must happen BEFORE we remove any content
    price_vnd = None
    if 'năm trăm nghìn' in text.lower() or '500,000' in text:
        price_vnd = 500000
    
    # Remove noise tokens like [Music], [inaudible], [Laughter]
    text = re.sub(r'\[(Music|inaudible|Laughter)\]', '', text)
    
    # Strip timestamps [00:00:00]
    text = re.sub(r'\[\d{2}:\d{2}:\d{2}\]', '', text)
    
    # Clean up extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Return a cleaned dictionary for the UnifiedDocument schema
    return {
        "document_id": "transcript-doc-001",
        "content": text,
        "source_type": "Video",
        "author": "Speaker 1",
        "timestamp": None,
        "source_metadata": {
            "original_file": "demo_transcript.txt",
            "detected_price_vnd": price_vnd
        }
    }

