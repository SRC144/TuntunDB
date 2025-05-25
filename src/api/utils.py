import json
from datetime import datetime, date
from typing import Any
import decimal

class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle bytes and other special types"""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, bytes):
            try:
                # Try UTF-8 first
                return obj.decode('utf-8').rstrip('\x00')
            except UnicodeDecodeError:
                # Fallback to hex representation for binary data
                return obj.hex()
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return str(obj)
        elif hasattr(obj, '__dict__'):
            # Handle custom objects by converting them to dict
            return obj.__dict__
        return super().default(obj) 