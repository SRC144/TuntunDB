import numpy as np
from datetime import datetime
from typing import Any, Union, Tuple, List
import struct
import pymorton  # For Z-order curve encoding of 2D points

# Constants
MAX_VARCHAR_LENGTH = 20  # Maximum number of characters to use for string hashing
MAX_UINT64 = 18446744073709551615  # Maximum value for unsigned 64-bit integer

class TypeConverter:
    """Handles conversion of different data types to numeric keys for indexing"""
    
    @staticmethod
    def to_numeric_key(value: Any, col_type: str) -> int:
        """Convert any supported type to a uint64 key suitable for indexing"""
        if value is None:
            return 0
            
        # Extract size for VARCHAR types
        size = None
        if col_type.startswith("VARCHAR"):
            try:
                size = int(col_type.split('[')[1].split(']')[0])
            except (IndexError, ValueError):
                size = MAX_VARCHAR_LENGTH
            
        # Handle ARRAY types
        if col_type.startswith("ARRAY"):
            base_type = col_type.split('[')[1].split(']')[0].lower()
            converter = getattr(TypeConverter, f"_array_{base_type}_to_key", None)
        else:
            # Get base type name for non-array types
            base_type = col_type.split('[')[0].lower()
            converter = getattr(TypeConverter, f"_{base_type}_to_key", None)
            
        if not converter:
            raise ValueError(f"Unsupported type for indexing: {col_type}")
            
        # Pass size parameter for VARCHAR
        if base_type == "varchar":
            return converter(value, size=size) & MAX_UINT64
        return converter(value) & MAX_UINT64
    
    @staticmethod
    def to_python_value(value: Any, col_type: str) -> Any:
        """Convert a string or raw value to appropriate Python type"""
        if value is None:
            return None
            
        # Extract size for VARCHAR types
        size = None
        if col_type.startswith("VARCHAR"):
            try:
                size = int(col_type.split('[')[1].split(']')[0])
            except (IndexError, ValueError):
                size = MAX_VARCHAR_LENGTH
            
        # Handle ARRAY types
        if col_type.startswith("ARRAY"):
            base_type = col_type.split('[')[1].split(']')[0].lower()
            converter = getattr(TypeConverter, f"_array_{base_type}_to_python", None)
        else:
            # Get base type name for non-array types
            base_type = col_type.split('[')[0].lower()
            converter = getattr(TypeConverter, f"_{base_type}_to_python", None)
            
        if not converter:
            raise ValueError(f"Unsupported type for conversion: {col_type}")
            
        # Pass size parameter for VARCHAR
        if base_type == "varchar":
            return converter(value, size=size)
        return converter(value)
    
    @staticmethod
    def to_binary_value(value: Any, col_type: str, size: int = None) -> Any:
        """Convert a value to its binary format for storage"""
        if value is None:
            return None
            
        # First convert to Python type if it's a string
        if isinstance(value, str):
            value = TypeConverter.to_python_value(value, col_type)
            
        # Handle ARRAY types
        if col_type.startswith("ARRAY"):
            base_type = col_type.split('[')[1].split(']')[0].lower()
            converter = getattr(TypeConverter, f"_array_{base_type}_to_binary", None)
        else:
            # Get base type name for non-array types
            base_type = col_type.split('[')[0].lower()
            converter = getattr(TypeConverter, f"_{base_type}_to_binary", None)
            
        if not converter:
            raise ValueError(f"Unsupported type for binary conversion: {col_type}")
            
        if base_type == "varchar":
            return converter(value, size=size)
        return converter(value)
    
    @staticmethod
    def create_format_string(columns: List[dict]) -> str:
        """Create struct format string from column definitions"""
        format_parts = ['=']  # Use native byte order
        
        for col in columns:
            col_type = col["type"]
            if col_type == "INT":
                format_parts.append('i')
            elif col_type.startswith("VARCHAR"):
                size = int(col_type.split('[')[1].split(']')[0])
                format_parts.append(f'{size}s')
            elif col_type == "DATE":
                format_parts.append('I')  # Unsigned int for timestamp
            elif col_type == "FLOAT":
                format_parts.append('f')
            elif col_type == "ARRAY[FLOAT]":
                format_parts.extend(['f', 'f'])  # Two floats for 2D point
                
        return ''.join(format_parts)
    
    # Conversion to numeric keys for indexing
    @staticmethod
    def _int_to_key(value: Union[int, str]) -> int:
        """Convert integer to uint64 key"""
        val = int(value)
        # Shift negative values into positive range
        if val < 0:
            val = (abs(val) << 1) | 1  # Set LSB to 1 for negative
        else:
            val = val << 1  # Set LSB to 0 for positive
        return val & MAX_UINT64
    
    @staticmethod
    def _float_to_key(value: Union[float, str]) -> int:
        """Convert float to uint64 key preserving ordering"""
        float_val = float(value)
        # Use numpy's float64 to handle NaN and Inf consistently
        float64_val = np.float64(float_val)
        # Convert to ordered integer using numpy's view
        int_val = float64_val.view(np.int64)
        # Handle negative values by flipping sign bit
        if int_val < 0:
            int_val = ~int_val
        return int_val & MAX_UINT64
    
    @staticmethod
    def _varchar_to_key(value: Union[str, bytes], size: int = None) -> int:
        """Convert string to uint64 key preserving lexicographical order"""
        if isinstance(value, bytes):
            value = value.decode().rstrip('\x00')
            
        # Use the specified size or default to MAX_VARCHAR_LENGTH
        max_length = size if size is not None else MAX_VARCHAR_LENGTH
        
        # Pad or truncate to fixed length
        text = value.ljust(max_length)[:max_length]
        
        # Convert to numpy array of ASCII codes
        ascii_codes = np.array([ord(c) & 0x7F for c in text], dtype=np.uint8)
        
        # Pack bits using numpy's packbits
        packed = np.packbits(ascii_codes)
        
        # Convert to integer ensuring uint64
        return int.from_bytes(packed.tobytes(), byteorder='big') & MAX_UINT64
    
    @staticmethod
    def _date_to_key(value: Union[str, int, datetime]) -> int:
        """Convert date to uint64 key (unix timestamp)"""
        if isinstance(value, str):
            dt = datetime.strptime(value, '%Y-%m-%d')
            return int(dt.timestamp()) & MAX_UINT64
        elif isinstance(value, datetime):
            return int(value.timestamp()) & MAX_UINT64
        return int(value) & MAX_UINT64
    
    @staticmethod
    def _array_float_to_key(value: Union[Tuple[float, float], List[float]]) -> int:
        """Convert array of floats to uint64 key using Z-order curve"""
        if isinstance(value, (list, tuple)) and len(value) == 2:
            x, y = map(float, value)
            # Scale and shift to handle negatives
            x = int((x + 1e6) * 1e6) & ((1 << 32) - 1)  # Use 32 bits per coordinate
            y = int((y + 1e6) * 1e6) & ((1 << 32) - 1)
            # Use Z-order curve to preserve spatial locality
            return pymorton.interleave2(x, y) & MAX_UINT64
        raise ValueError("Array[FLOAT] keys must be 2D points")
    
    # Conversion to Python types
    @staticmethod
    def _int_to_python(value: Union[int, str]) -> int:
        return int(value)
    
    @staticmethod
    def _float_to_python(value: Union[float, str]) -> float:
        return float(value)
    
    @staticmethod
    def _varchar_to_python(value: Union[str, bytes], size: int = None) -> str:
        """Convert VARCHAR to Python string"""
        if isinstance(value, bytes):
            return value.decode().rstrip('\x00')
        return str(value)
    
    @staticmethod
    def _date_to_python(value: Union[str, int, datetime]) -> int:
        if isinstance(value, str):
            dt = datetime.strptime(value, '%Y-%m-%d')
            return int(dt.timestamp())
        elif isinstance(value, datetime):
            return int(value.timestamp())
        return int(value)
    
    @staticmethod
    def _array_float_to_python(value: Union[str, Tuple[float, float], list]) -> Tuple[float, float]:
        if isinstance(value, str):
            x, y = map(float, value.strip('()').split(','))
            return (x, y)
        elif isinstance(value, (list, tuple)):
            return tuple(map(float, value[:2]))
        raise ValueError("Invalid 2D point format")
    
    # Conversion to binary format
    @staticmethod
    def _int_to_binary(value: int) -> int:
        return value
    
    @staticmethod
    def _float_to_binary(value: float) -> float:
        return value
    
    @staticmethod
    def _varchar_to_binary(value: Union[str, bytes], size: int = None) -> bytes:
        """Convert VARCHAR to binary format"""
        if isinstance(value, str):
            if size is None:
                size = MAX_VARCHAR_LENGTH
            return value.encode().ljust(size, b'\x00')
        return value
    
    @staticmethod
    def _date_to_binary(value: int) -> int:
        return value
    
    @staticmethod
    def _array_float_to_binary(value: Tuple[float, float]) -> Tuple[float, float]:
        return value

# Example usage:
# numeric_key = TypeConverter.to_numeric_key("hello", "VARCHAR")
# numeric_key = TypeConverter.to_numeric_key(3.14, "FLOAT")
# numeric_key = TypeConverter.to_numeric_key("2024-01-01", "DATE")
# numeric_key = TypeConverter.to_numeric_key((1.5, 2.5), "ARRAY[FLOAT]") 