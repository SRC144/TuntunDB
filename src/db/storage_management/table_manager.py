import os
import json
import struct
from typing import Dict, Any, Optional, List
from ..cursors.line_cursor import LineCursor

class TableManager:
    # Constant for deletion marker size (1 byte for deleted flag)
    DELETION_MARKER_SIZE = 1

    def __init__(self, data_dir: str):
        # Use src/data as the base directory
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")
        os.makedirs(self.data_dir, exist_ok=True)
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        # Convert table name to lowercase
        table_name = table_name.lower()
        table_dir = os.path.join(self.data_dir, table_name)
        meta_file = os.path.join(table_dir, "meta.json")
        exists = os.path.exists(meta_file)
        print(f"[DEBUG] table_exists check for {table_name}: {exists}")  # DEBUG
        return exists
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get table metadata including schema, indexes, and file paths"""
        # Convert table name to lowercase
        table_name = table_name.lower()
        table_dir = os.path.join(self.data_dir, table_name)
        meta_file = os.path.join(table_dir, "meta.json")
        
        if not os.path.exists(meta_file):
            return None
            
        with open(meta_file, 'r') as f:
            return json.load(f)
    
    def create_table(self, table_name: str, columns: List[Dict], indexes: Dict = None, primary_key: str = None) -> Dict:
        """Create a new table with the specified columns and indexes"""
        print(f"[DEBUG] Starting create_table for {table_name}")
        
        # Convert table name to lowercase
        table_name = table_name.lower()
        
        if self.table_exists(table_name):
            print(f"[DEBUG] Table {table_name} already exists, returning error")
            return {
                "status": "error",
                "message": f"Table {table_name} already exists"
            }

        try:
            # Create table directory
            table_dir = os.path.join(self.data_dir, table_name)
            os.makedirs(table_dir, exist_ok=True)
            
            # Create format string for actual data (without deletion marker)
            format_str = self._create_format_string(columns)
            # Prepend deletion marker byte to format string (but not to columns)
            binary_format = f"={self.DELETION_MARKER_SIZE}s{format_str[1:]}"
            
            # Create table info
            table_info = {
                "name": table_name,
                "columns": columns,  # Only actual columns
                "format_str": binary_format,  # Internal format includes marker
                "record_format": format_str,  # Format for just the data portion
                "data_file": os.path.join(table_dir, "data.bin"),
                "indexes": indexes or {},
                "primary_key": primary_key,
                "index_files": {
                    col: os.path.join(table_dir, f"{col}_{index_type}.idx")
                    for col, index_type in (indexes or {}).items()
                },
                "stats": {
                    "total_records": 0,
                    "deleted_records": 0,
                    "last_compaction": None
                }
            }
            
            # Save table info
            self._save_table_info(table_name, table_info)
            
            # Create empty data file
            with open(table_info["data_file"], 'wb') as f:
                pass
                
            # Create empty index files
            for col, index_type in (indexes or {}).items():
                index_file = table_info["index_files"][col]
                with open(index_file, 'wb') as f:
                    pass
                    
                # For sequential indexes, create auxiliary file
                if index_type.lower() == 'sequential':
                    aux_file = f"{index_file}.aux"
                    with open(aux_file, 'wb') as f:
                        pass
            
            return table_info
            
        except Exception as e:
            print(f"[DEBUG] Error in create_table: {str(e)}")
            return {
                "status": "error",
                "message": f"Failed to create table: {str(e)}"
            }

    def update_table_stats(self, table_name: str, total_delta: int = 0, deleted_delta: int = 0) -> None:
        """Update table statistics for record counts"""
        table_info = self.get_table_info(table_name)
        if not table_info:
            return
        
        table_info["stats"]["total_records"] += total_delta
        table_info["stats"]["deleted_records"] += deleted_delta
        self._save_table_info(table_name, table_info)

    def should_compact(self, table_name: str) -> bool:
        """Check if table should be compacted based on deletion ratio"""
        table_info = self.get_table_info(table_name)
        if not table_info:
            return False
            
        stats = table_info["stats"]
        total = stats["total_records"]
        deleted = stats["deleted_records"]
        
        if total == 0:
            return False
            
        return (deleted / total) > 0.2  # 20% threshold

    def _save_table_info(self, table_name: str, table_info: Dict[str, Any]) -> None:
        """Save table metadata to file"""
        # Convert table name to lowercase
        table_name = table_name.lower()
        table_dir = os.path.join(self.data_dir, table_name)
        meta_file = os.path.join(table_dir, "meta.json")
        
        with open(meta_file, 'w') as f:
            json.dump(table_info, f, indent=2)
    
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
    
    def append_record(self, table_name: str, record: bytes) -> Dict[str, Any]:
        """Append a binary record to the table's data file and return its offset"""
        table_info = self.get_table_info(table_name)
        if not table_info:
            return {
                "status": "error",
                "message": f"Table {table_name} not found"
            }
            
        try:
            # Add deletion marker (0 for not deleted)
            record_with_marker = b'\x00' + record
            
            with open(table_info["data_file"], 'ab') as f:
                offset = f.tell()
                f.write(record_with_marker)
                return {
                    "status": "success",
                    "offset": offset
                }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to append record: {str(e)}"
            }

    def get_all_tables(self) -> List[str]:
        """Get list of all available tables by checking directories in data folder"""
        try:
            # Get all directories in the data folder - each directory is a table
            return sorted([
                d for d in os.listdir(self.data_dir)
                if os.path.isdir(os.path.join(self.data_dir, d))
            ])
        except Exception as e:
            print(f"Error getting tables: {str(e)}")
            return []
