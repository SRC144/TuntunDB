from typing import Dict, Any, List
from ..cursors.line_cursor import LineCursor
from ..index_handling.index_factory import IndexFactory
from ..storage_management.type_conversion import TypeConverter

class SelectCommand:
    def __init__(self, table_info: Dict[str, Any]):
        self.table_info = table_info
        self.cursor = LineCursor(table_info)
        # For now, only use B+ tree index for testing
        self.index_type_map = {
            'BPlusTree': 'bplus'  # Updated from BTree to BPlusTree
        }
    
    def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a SELECT query"""
        if "filter" in params:
            records = self._get_records_with_index(params["filter"])
        else:
            records = self._get_all_records()
            
        return {
            "status": "success",
            "records": records
        }
    
    def _get_all_records(self) -> List[List[Any]]:
        """Get all records from the table by reading sequentially"""
        records = []
        with self.cursor as cursor:
            # Get total number of records
            total = cursor.total_lines()
            # Read each record
            for i in range(total):
                cursor.goto_line(i)
                record = cursor.read_record(i)
                if record:
                    records.append(record)
        return records
    
    def _get_records_with_index(self, filter: Dict[str, Any]) -> List[List[Any]]:
        """Get records using an index"""
        col = filter["column"]
        index_file = self.table_info["index_files"][col]
        index_type = self.table_info["indexes"][col]
        
        # Get column position and type
        col_idx = next(
            (i for i, c in enumerate(self.table_info["columns"]) if c["name"] == col),
            -1
        )
        if col_idx == -1:
            return []
            
        col_type = self.table_info["columns"][col_idx]["type"]
        
        # Convert filter value(s) to numeric key(s)
        if filter["operation"] == "=":
            key = TypeConverter.to_numeric_key(filter["value"], col_type)
            index = IndexFactory.get_index(
                index_type='bplus',
                index_filename=index_file,
                data_filename=self.table_info["data_file"],
                data_format=self.table_info["format_str"],
                key_position=col_idx
            )
            positions = index.search(key)  # Changed from find to search to match BPlusTreeIndex method
        elif filter["operation"] == "BETWEEN":
            low_key = TypeConverter.to_numeric_key(filter["from"], col_type)
            high_key = TypeConverter.to_numeric_key(filter["to"], col_type)
            index = IndexFactory.get_index(
                index_type='bplus',
                index_filename=index_file,
                data_filename=self.table_info["data_file"],
                data_format=self.table_info["format_str"],
                key_position=col_idx
            )
            positions = index.range_search(low_key, high_key)  # Changed from range to range_search to match BPlusTreeIndex method
        else:
            return []
            
        # Read records at found positions
        records = []
        with self.cursor as cursor:
            for pos in positions:
                cursor.goto_line(pos)
                record = cursor.read_record(pos)
                if record:
                    records.append(record)
        return records
    
    def _matches_conditions(self, record: List[Any], conditions: List[Dict[str, Any]]) -> bool:
        """Check if a record matches all conditions"""
        for cond in conditions:
            col_idx = next(
                (i for i, c in enumerate(self.table_info["columns"]) if c["name"] == cond["column"]),
                -1
            )
            if col_idx == -1:
                continue
                
            col_type = self.table_info["columns"][col_idx]["type"]
            record_value = record[col_idx]
            
            # Convert condition value to Python type for comparison
            cond_value = TypeConverter.to_python_value(cond["value"], col_type)
            
            if cond["operation"] == "=":
                if record_value != cond_value:
                    return False
            elif cond["operation"] == ">":
                if record_value <= cond_value:
                    return False
            elif cond["operation"] == "<":
                if record_value >= cond_value:
                    return False
            elif cond["operation"] == ">=":
                if record_value < cond_value:
                    return False
            elif cond["operation"] == "<=":
                if record_value > cond_value:
                    return False
                    
        return True
