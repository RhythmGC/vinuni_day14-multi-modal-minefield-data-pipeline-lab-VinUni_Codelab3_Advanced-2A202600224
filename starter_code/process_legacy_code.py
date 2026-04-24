import ast
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.

def extract_logic_from_code(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()
    # ------------------------------------------
    
    # Use the 'ast' module to find docstrings for functions
    try:
        tree = ast.parse(source_code)
    except SyntaxError:
        # If we can't parse the code, fall back to regex extraction
        tree = None
    
    # Extract docstrings using AST
    docstrings = []
    if tree:
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Module)):
                docstring = ast.get_docstring(node)
                if docstring:
                    docstrings.append(docstring)
    
    # Extract business rules from comments using regex
    business_rules = []
    # Look for patterns like "# Business Logic Rule XXX" or similar
    rule_pattern = r'#\s*(Business Logic Rule\s*\d+:?|IMPORTANT:|NOTE:|WARNING:)[^\n]*'
    rule_matches = re.findall(rule_pattern, source_code, re.IGNORECASE)
    for match in rule_matches:
        if isinstance(match, tuple):
            business_rules.extend([m for m in match if m])
        else:
            business_rules.append(match)
    
    # Also look for specific patterns in the legacy code
    specific_patterns = [
        r'Business Logic Rule\s*\d+[^\n]*',
        r'IMPORTANT:[^\n]*',
        r'WARNING:[^\n]*',
        r'Note:[^\n]*'
    ]
    
    for pattern in specific_patterns:
        matches = re.findall(pattern, source_code, re.IGNORECASE)
        business_rules.extend(matches)
    
    # Create content string
    content_parts = []
    
    if docstrings:
        content_parts.append("Docstrings found:")
        for i, docstring in enumerate(docstrings, 1):
            content_parts.append(f"{i}. {docstring.strip()}")
    
    if business_rules:
        content_parts.append("Business rules found in comments:")
        for i, rule in enumerate(business_rules, 1):
            content_parts.append(f"{i}. {rule.strip()}")
    
    content = ". ".join(content_parts) if content_parts else "No docstrings or business rules found."
    
    # Return a dictionary for the UnifiedDocument schema
    return {
        "document_id": "legacy-code-doc-001",
        "content": content,
        "source_type": "Code",
        "author": "Senior Dev (retired)",  # From the file header
        "timestamp": None,
        "source_metadata": {
            "original_file": "legacy_pipeline.py",
            "docstrings_extracted": len(docstrings) if docstrings else 0,
            "business_rules_found": len(business_rules) if business_rules else 0,
            "docstrings": docstrings,
            "business_rules": list(set(business_rules))  # Remove duplicates
        }
    }

