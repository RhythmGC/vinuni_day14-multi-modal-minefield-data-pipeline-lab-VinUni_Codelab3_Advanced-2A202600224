from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

# ==========================================
# ROLE 1: LEAD DATA ARCHITECT
# ==========================================
# Your task is to define the Unified Schema for all sources.
# This is v1. Note: A breaking change is coming at 11:00 AM!

class UnifiedDocument(BaseModel):
    # Core identifying fields
    document_id: str
    content: str
    source_type: str  # e.g., 'PDF', 'Video', 'HTML', 'CSV', 'Code'
    
    # Optional fields
    author: Optional[str] = "Unknown"
    timestamp: Optional[datetime] = None
    
    # Source-specific metadata as a flexible dictionary
    source_metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        # Allow extra fields for forward compatibility
        extra = "allow"
