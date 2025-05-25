import os
import json
from typing import Dict, Any, Optional, List
import struct
from ..index_handling.index_factory import IndexFactory
from .type_conversion import TypeConverter

class TableManager:
    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(data_dir, exist_ok=True)
        
        # Only B+ tree is implemented for now
        self.index_type_map = {
            'BPlusTree': 'bplus'  # Only mapping we support currently
        }
    
    def _convert_to_numeric_key(self, value: Any, col_type: str) -> int:
        """Convert a value to a numeric key suitable for indexing"""
        return TypeConverter.to_numeric_key(value, col_type)
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get table metadata including schema, indexes, and file paths"""
        table_dir = os.path.join(self.data_dir, table_name)
        meta_file = os.path.join(table_dir, "meta.json")
        
        if not os.path.exists(meta_file):
            return None
            
        with open(meta_file, 'r') as f:
            return json.load(f)
    
    def create_table(self, table_name: str, columns: list, indexes: Dict[str, str], primary_key: str = None) -> Dict[str, Any]:
        """Create a new table with the given schema"""
        # Create table directory
        table_dir = os.path.join(self.data_dir, table_name)
        os.makedirs(table_dir, exist_ok=True)
        
        # Create format string for struct packing
        format_str = self._create_format_string(columns)
        
        # Create metadata
        metadata = {
            "name": table_name,
            "columns": columns,
            "indexes": indexes,
            "primary_key": primary_key,
            "format_str": format_str,
            "data_file": os.path.join(table_dir, "data.bin"),
            "index_files": {
                col: os.path.join(table_dir, f"{col}.idx")
                for col in indexes.keys()
            }
        }
        
        # Save metadata
        meta_file = os.path.join(table_dir, "meta.json")
        with open(meta_file, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        # Create empty data file
        with open(metadata["data_file"], 'wb') as f:
            pass
            
        # Create index directories and files
        for col, index_file in metadata["index_files"].items():
            # Skip non-BPlusTree indexes for now
            if indexes[col] != 'BPlusTree':
                continue
                
            # Create index directory if needed
            index_dir = os.path.dirname(index_file)
            os.makedirs(index_dir, exist_ok=True)
            
            # Get column position for key extraction
            key_position = next(
                (i for i, c in enumerate(columns) if c["name"] == col),
                0
            )
                
            try:
                IndexFactory.get_index(
                    index_type='bplus',  # Always use bplus for BPlusTree
                    index_filename=metadata["index_files"][col],
                    data_filename=metadata["data_file"],
                    data_format=format_str,
                    key_position=key_position
                )
            except Exception as e:
                print(f"Warning: Failed to create index {indexes[col]} for column {col}: {e}")
                
        return metadata
    
    def create_table_from_file(self, table_name: str, file_path: str, index_info: Dict[str, Any]) -> Dict[str, Any]:
        """Create a table from a CSV file with index"""
        # For simplicity, assume CSV has headers and we can infer types
        # In a real implementation, we would need more sophisticated type inference
        import csv
        
        table_dir = os.path.join(self.data_dir, table_name)
        os.makedirs(table_dir, exist_ok=True)
        
        # Read CSV headers and first row to infer types
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            first_row = next(reader)
        
        # Infer column types (very basic inference)
        columns = []
        for i, (header, value) in enumerate(zip(headers, first_row)):
            col_type = self._infer_type(value)
            columns.append({
                "name": header,
                "type": col_type
            })
        
        # Create indexes dict with the specified index
        indexes = {
            index_info["column"]: index_info["type"]
        }
        
        # Create the table metadata and indexes
        metadata = self.create_table(table_name, columns, indexes)
        
        # Import data from CSV and build indexes
        with open(metadata["data_file"], 'wb') as data_file:
            with open(file_path, 'r') as csv_file:
                reader = csv.reader(csv_file)
                next(reader)  # Skip header
                
                for row in reader:
                    # Convert row data to binary format
                    record = self._convert_row_to_binary(row, columns)
                    data_file.write(record)
        
        # Rebuild all indexes
        self._rebuild_indexes(metadata)
        
        return metadata
    
    def _rebuild_indexes(self, metadata: Dict[str, Any]):
        """Rebuild all indexes for a table"""
        for col, index_type in metadata["indexes"].items():
            # Skip non-BPlusTree indexes for now
            if index_type != 'BPlusTree':
                continue
                
            # Get column position
            key_position = next(
                (i for i, c in enumerate(metadata["columns"]) if c["name"] == col),
                0
            )
                
            try:
                # Create and build index
                IndexFactory.get_index(
                    index_type='bplus',  # Always use bplus for BPlusTree
                    index_filename=metadata["index_files"][col],
                    data_filename=metadata["data_file"],
                    data_format=metadata["format_str"],
                    key_position=key_position
                )
            except Exception as e:
                print(f"Warning: Failed to rebuild index {index_type} for column {col}: {e}")
    
    def _convert_row_to_binary(self, row: list, columns: list) -> bytes:
        """Convert a CSV row to binary format"""
        values = []
        for value, col in zip(row, columns):
            col_type = col["type"]
            if col_type == 'INT':
                values.append(int(value))
            elif col_type == 'FLOAT':
                values.append(float(value))
            elif col_type.startswith('VARCHAR'):
                size = int(col_type.split('[')[1].split(']')[0])
                values.append(value.encode().ljust(size, b'\x00'))
            elif col_type == 'DATE':
                # Convert date to unix timestamp
                from datetime import datetime
                dt = datetime.strptime(value, '%Y-%m-%d')
                values.append(int(dt.timestamp()))
            elif col_type == 'ARRAY[FLOAT]':
                # Assume 2D point for now
                x, y = map(float, value.strip('()').split(','))
                values.extend([x, y])
                
        return struct.pack(self._create_format_string(columns), *values)
    
    def _create_format_string(self, columns: list) -> str:
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
    
    def _infer_type(self, value: str) -> str:
        """Very basic type inference"""
        try:
            int(value)
            return 'INT'
        except ValueError:
            try:
                float(value)
                return 'FLOAT'
            except ValueError:
                if ',' in value and value.count(',') == 1:
                    # Assume it's a 2D point
                    return 'ARRAY[FLOAT]'
                else:
                    # Default to VARCHAR with some padding
                    return 'VARCHAR[50]'
    
    def insert_record(self, table_info: Dict[str, Any], values: List[Any]) -> None:
        """Insert a record and update all indexes"""
        # Convert values to appropriate types
        converted_values = []
        for value, col in zip(values, table_info["columns"]):
            col_type = col["type"]
            
            if col_type == "INT":
                converted_values.append(int(value))
            elif col_type == "FLOAT":
                converted_values.append(float(value))
            elif col_type.startswith("VARCHAR"):
                size = int(col_type.split('[')[1].split(']')[0])
                converted_values.append(value.encode().ljust(size, b'\x00'))
            elif col_type == "DATE":
                from datetime import datetime
                dt = datetime.strptime(value, '%Y-%m-%d')
                converted_values.append(int(dt.timestamp()))
            elif col_type == "ARRAY[FLOAT]":
                x, y = map(float, value.split(','))
                converted_values.append((x, y))
            else:
                converted_values.append(value)
        
        # Pack values into binary format
        pack_values = []
        for val in converted_values:
            if isinstance(val, tuple):
                pack_values.extend(val)
            else:
                pack_values.append(val)
                
        record = struct.pack(table_info["format_str"], *pack_values)
        
        # Write record to data file
        with open(table_info["data_file"], 'ab') as f:
            f.seek(0, 2)  # Seek to end
            record_pos = f.tell() // struct.calcsize(table_info["format_str"])
            f.write(record)
        
        # Update indexes
        for col, index_file in table_info["index_files"].items():
            index_type = table_info["indexes"][col]
            # Skip non-BPlusTree indexes for now
            if index_type != 'BPlusTree':
                continue
                
            # Get column position and value
            col_idx = next(
                (i for i, c in enumerate(table_info["columns"]) if c["name"] == col),
                -1
            )
            if col_idx == -1:
                continue
                
            value = converted_values[col_idx]
            col_type = table_info["columns"][col_idx]["type"]
            
            try:
                # Convert value to numeric key using TypeConverter
                key = TypeConverter.to_numeric_key(value, col_type)
                
                index = IndexFactory.get_index(
                    index_type='bplus',  # Always use bplus for BPlusTree
                    index_filename=index_file,
                    data_filename=table_info["data_file"],
                    data_format=table_info["format_str"],
                    key_position=col_idx
                )
                
                # Add to index using numeric key
                index.add(key, record_pos)
            except Exception as e:
                print(f"Warning: Failed to update index for column {col}: {e}")
