import struct
from datetime import datetime
from typing import Any, List

class TypeConverter:
    @staticmethod
    def convert_value(value: Any, col_type: str) -> Any:
        """Convert a value to its appropriate type based on column definition"""
        if col_type == "INT":
            return int(value)
        elif col_type == "FLOAT":
            return float(value)
        elif col_type.startswith("VARCHAR"):
            size = int(col_type.split('[')[1].split(']')[0])
            return value.encode().ljust(size, b'\x00')
        elif col_type == "DATE":
            dt = datetime.strptime(value, '%Y-%m-%d')
            return int(dt.timestamp())
        elif col_type == "ARRAY[FLOAT]":
            x, y = map(float, value.split(','))
            return (x, y)
        else:
            return value

    @staticmethod
    def convert_record(values: List[Any], columns: List[dict], format_str: str) -> bytes:
        """Convert a list of values to binary record format"""
        
        # Convert values to appropriate types
        converted_values = []
        # Add deletion marker (0 for not deleted)
        converted_values.append(b'\x00')  # Add deletion marker first
        
        # Convert actual data values
        for value, col in zip(values, columns):
            converted = TypeConverter.convert_value(value, col["type"])
            if isinstance(converted, tuple):
                converted_values.extend(converted)
            else:
                converted_values.append(converted)

        # Pack into binary format
        return struct.pack(format_str, *converted_values)

    @staticmethod
    def bytes_to_values(raw_record: bytes, format_str: str, columns: List[dict]) -> List[Any]:
        """Convert a binary record back to Python values"""
        # Unpack raw bytes into tuple of values
        values = struct.unpack(format_str, raw_record)
        
        # Convert each value back to its Python type, skipping deletion marker
        result = []
        value_idx = 1  # Skip deletion marker
        
        for col in columns:
            col_type = col["type"]
            
            if col_type == "INT":
                result.append(values[value_idx])
                value_idx += 1
            elif col_type == "FLOAT":
                result.append(values[value_idx])
                value_idx += 1
            elif col_type.startswith("VARCHAR"):
                # Convert bytes to string and strip null bytes
                str_val = values[value_idx].rstrip(b'\x00').decode()
                result.append(str_val)
                value_idx += 1
            elif col_type == "DATE":
                # Convert timestamp to date string
                dt = datetime.fromtimestamp(values[value_idx])
                result.append(dt.strftime('%Y-%m-%d'))
                value_idx += 1
            elif col_type == "ARRAY[FLOAT]":
                # Combine two floats into coordinate tuple
                result.append(f"{values[value_idx]},{values[value_idx + 1]}")
                value_idx += 2
            else:
                result.append(values[value_idx])
                value_idx += 1
                
        return result 