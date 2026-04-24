import json
import time
import os
from datetime import datetime

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")


# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion pipeline and handle errors/SLA.

def main():
    start_time = time.time()
    final_kb = []
    
    # --- FILE PATH SETUP (Handled for students) ---
    pdf_path = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")
    
    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")
    # ----------------------------------------------
    
    # Process PDF
    print("Processing PDF...")
    pdf_doc = extract_pdf_data(pdf_path)
    if pdf_doc and run_quality_gate(pdf_doc):
        # Convert to UnifiedDocument for validation
        try:
            unified_doc = UnifiedDocument(**pdf_doc)
            final_kb.append(unified_doc.dict())
            print(f"✓ PDF processed successfully: {unified_doc.document_id}")
        except Exception as e:
            print(f"✗ PDF validation failed: {e}")
    else:
        print("✗ PDF processing failed or failed quality gate")
    
    # Process Transcript
    print("Processing transcript...")
    trans_doc = clean_transcript(trans_path)
    if trans_doc and run_quality_gate(trans_doc):
        try:
            unified_doc = UnifiedDocument(**trans_doc)
            final_kb.append(unified_doc.dict())
            print(f"✓ Transcript processed successfully: {unified_doc.document_id}")
        except Exception as e:
            print(f"✗ Transcript validation failed: {e}")
    else:
        print("✗ Transcript processing failed or failed quality gate")
    
    # Process HTML
    print("Processing HTML catalog...")
    html_docs = parse_html_catalog(html_path)
    for i, doc in enumerate(html_docs):
        if doc and run_quality_gate(doc):
            try:
                unified_doc = UnifiedDocument(**doc)
                final_kb.append(unified_doc.dict())
                print(f"✓ HTML product {i+1} processed successfully: {unified_doc.document_id}")
            except Exception as e:
                print(f"✗ HTML product {i+1} validation failed: {e}")
        else:
            print(f"✗ HTML product {i+1} processing failed or failed quality gate")
    
    # Process CSV
    print("Processing CSV sales records...")
    csv_docs = process_sales_csv(csv_path)
    for i, doc in enumerate(csv_docs):
        if doc and run_quality_gate(doc):
            try:
                unified_doc = UnifiedDocument(**doc)
                final_kb.append(unified_doc.dict())
                print(f"✓ CSV record {i+1} processed successfully: {unified_doc.document_id}")
            except Exception as e:
                print(f"✗ CSV record {i+1} validation failed: {e}")
        else:
            print(f"✗ CSV record {i+1} processing failed or failed quality gate")
    
    # Process Legacy Code
    print("Processing legacy code...")
    code_doc = extract_logic_from_code(code_path)
    if code_doc and run_quality_gate(code_doc):
        try:
            unified_doc = UnifiedDocument(**code_doc)
            final_kb.append(unified_doc.dict())
            print(f"✓ Legacy code processed successfully: {unified_doc.document_id}")
        except Exception as e:
            print(f"✗ Legacy code validation failed: {e}")
    else:
        print("✗ Legacy code processing failed or failed quality gate")
    
    # Save final knowledge base
    print(f"Saving {len(final_kb)} documents to {output_path}...")
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(final_kb, f, indent=2, ensure_ascii=False, default=str)
        print("✓ Knowledge base saved successfully")
    except Exception as e:
        print(f"✗ Failed to save knowledge base: {e}")
    
    end_time = time.time()
    print(f"Pipeline finished in {end_time - start_time:.2f} seconds.")
    print(f"Total valid documents stored: {len(final_kb)}")


if __name__ == "__main__":
    main()
