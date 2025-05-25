from typing import Dict, Any, List
from ..cursors.line_cursor import LineCursor
from ..index_handling.index_factory import IndexFactory
from ..storage_management.type_conversion import TypeConverter

class InsertCommand:
    def __init__(self, table_info: Dict[str, Any]):
        self.table_info = table_info
        self.cursor = LineCursor(table_info)
        # For now, only use B+ tree index for testing
        self.index_type_map = {
            'BPlusTree': 'bplus'
        }
    
    def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an INSERT query"""
        values = params["values"]
        
        # Convert values to appropriate Python types
        converted_values = []
        for value, col in zip(values, self.table_info["columns"]):
            col_type = col["type"]
            converted_values.append(TypeConverter.to_python_value(value, col_type))
        
        # Add record and get its position
        with self.cursor as cursor:
            record_pos = cursor.append_record(converted_values)
        
        # Update indexes
        for col, index_file in self.table_info["index_files"].items():
            index_type = self.table_info["indexes"][col]
            if index_type != 'BPlusTree':
                continue
                
            # Get column position and value
            col_idx = next(
                (i for i, c in enumerate(self.table_info["columns"]) if c["name"] == col),
                -1
            )
            if col_idx == -1:
                continue
                
            value = converted_values[col_idx]
            col_type = self.table_info["columns"][col_idx]["type"]
            
            try:
                # Convert value to numeric key
                key = TypeConverter.to_numeric_key(value, col_type)
                
                index = IndexFactory.get_index(
                    index_type='bplus',
                    index_filename=index_file,
                    data_filename=self.table_info["data_file"],
                    data_format=self.table_info["format_str"],
                    key_position=col_idx
                )
                
                # Add to index using numeric key
                index.add(key, record_pos)
            except Exception as e:
                print(f"Warning: Failed to update index for column {col}: {e}")
                
        return {
            "status": "success",
            "message": "Record inserted successfully",
            "record_position": record_pos
        } 