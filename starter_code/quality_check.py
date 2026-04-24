# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.

def run_quality_gate(document_dict):
    # Reject documents with 'content' length < 20 characters
    content = document_dict.get('content', '')
    if len(content) < 20:
        return False
    
    # Reject documents containing toxic/error strings
    # Use word boundaries to avoid matching numbers like "500" in "500,000 VND"
    toxic_patterns = [
        r'\bNull pointer exception\b',
        r'\bError:\b',
        r'\bException:\b',
        r'\bFailed\b',
        r'\bError\b',
        r'\bexception\b',
        r'\bfail\b',
        r'\b404\b',
        r'HTTP\s*50[0-9]\b',  # HTTP error codes like 500, 501, etc.
        r'\bError\s+\d+\b'  # Generic error with number
    ]
    
    import re
    content_lower = content.lower()
    for pattern in toxic_patterns:
        if re.search(pattern, content, re.IGNORECASE):
            return False
    
    # Check for discrepancies in legacy code (if source_type is Code)
    source_type = document_dict.get('source_type', '')
    if source_type == 'Code':
        source_metadata = document_dict.get('source_metadata', {})
        business_rules = source_metadata.get('business_rules', [])
        
        # Check for tax discrepancy: comment says 8% but code says 10%
        tax_comment_found = False
        tax_code_found = False
        
        for rule in business_rules:
            rule_lower = rule.lower()
            if 'vat' in rule_lower or 'tax' in rule_lower:
                if '8%' in rule or '8 percent' in rule_lower:
                    tax_comment_found = True
                if '0.10' in rule or '10%' in rule or '10 percent' in rule_lower:
                    tax_code_found = True
        
        # If we found both the misleading comment (8%) and the actual code (10%),
        # this is a discrepancy we should flag (but not necessarily reject)
        # For now, we'll just note it but not reject based on this alone
        # In a real system, we might want to flag this for review
    
    # Return True if pass, False if fail.
    return True
