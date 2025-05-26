from typing import Dict, Any, List
from ..cursors.line_cursor import LineCursor
from ..index_handling.index_factory import IndexFactory
from ..utils.type_converter import TypeConverter
from ..storage_management.table_manager import TableManager
import struct
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class SelectCommand:
    def __init__(self, table_manager: TableManager):
        self.table_manager = table_manager
    
    def _prepare_search_key(self, key: Any, col_type: str) -> bytes:
        """Convert a key to the proper byte format for index searching"""
        if isinstance(key, int):
            return key.to_bytes(8, byteorder='little')
        elif isinstance(key, str):
            return key.encode().ljust(8, b'\x00')
        elif isinstance(key, bytes):
            return key[:8].ljust(8, b'\x00')
        elif isinstance(key, float):
            return struct.pack('=d', key)  # 8-byte double
        else:
            raise ValueError(f"Unsupported key type for index: {type(key)}")
    
    def execute(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a SELECT query"""
        table_name = parsed_query["table_name"]
        
        # Get table info
        table_info = self.table_manager.get_table_info(table_name)
        if not table_info:
            logger.error(f"Table {table_name} not found")
            raise Exception(f"Table {table_name} not found")
            
            
        # Create cursor for this operation with just filename and record size
        record_size = struct.calcsize(table_info["format_str"])
        cursor = LineCursor(table_info["data_file"], record_size)
        
        # Check if we have any indexes available
        available_indexes = table_info.get("indexes", {})
        
        # Get records based on query type
        if "filters" in parsed_query and parsed_query["filters"]:
            records = self._get_filtered_records(cursor, table_info, parsed_query["filters"][0])
        else:
            records = self._get_all_records(cursor, table_info)
            
        result = {
            "status": "success",
            "table_name": table_name,
            "columns": table_info["columns"],
            "records": records
        }
        return result
    
    def _get_all_records(self, cursor: LineCursor, table_info: Dict[str, Any]) -> List[List[Any]]:
        """Get all records from the table by reading sequentially"""
        records = []
        with cursor as c:
            # Get total number of records
            total = c.total_records()
            
            # Read each record
            for i in range(total):
                c.goto_record(i)
                raw_record = c.read_record()
                if raw_record:
                    # Check deletion marker (first byte)
                    if raw_record[0] == b'\x00'[0]:  # Not deleted
                        # Convert raw bytes to Python values
                        record = TypeConverter.bytes_to_values(
                            raw_record, 
                            table_info["format_str"],
                            table_info["columns"]
                        )
                        records.append(record)
        return records
    
    def _get_records_with_index(self, table_info: Dict[str, Any], cursor: LineCursor, filter: Dict[str, Any]) -> List[List[Any]]:
        """Get records using an index"""
        col = filter["column"]
        
        # Get column position and type
        col_idx = next(
            (i for i, c in enumerate(table_info["columns"]) if c["name"] == col),
            -1
        )
        if col_idx == -1:
            return []
            
        col_type = table_info["columns"][col_idx]["type"]
        
        # Determine which index to use
        index_type = self._select_index_type(table_info, col, filter)
        logger.debug(f"Index type: {index_type}")
        if not index_type:
            return self._get_all_records(cursor, table_info)
            
        logger.debug(f"Selected index type {index_type} for column {col}")
        index_file = table_info["index_files"][col]
        
        # Create the index
        index = IndexFactory.get_index(
            index_type=index_type,
            index_filename=index_file,
            data_filename=table_info["data_file"],
            data_format=table_info["format_str"],
            key_position=col_idx
        )
        
        # Keep cursor open for all operations
        with cursor as c:
            records = []
            
            if filter["operation"] == "=":
                key = TypeConverter.to_index_key(filter["value"], col_type)
                result = index.search(key)
                if result is not None:
                    raw_record = c.read_at(result)
                    if raw_record and raw_record[0] == b'\x00'[0]:  # Not deleted
                        record = TypeConverter.bytes_to_values(
                            raw_record,
                            table_info["format_str"],
                            table_info["columns"]
                        )
                        records.append(record)
            elif filter["operation"] == "BETWEEN":
                low_key = TypeConverter.to_index_key(filter["from"], col_type)
                high_key = TypeConverter.to_index_key(filter["to"], col_type)
                positions = index.range_search(low_key, high_key)
                for pos in positions:
                    raw_record = c.read_at(pos)
                    if raw_record and raw_record[0] == b'\x00'[0]:  # Not deleted
                        record = TypeConverter.bytes_to_values(
                            raw_record,
                            table_info["format_str"],
                            table_info["columns"]
                        )
                        records.append(record)
            elif filter["operation"] == "SCAN":
                # Full table scan using B+ tree index
                total = c.total_records()
                for i in range(total):
                    raw_record = c.read_at(i)
                    if raw_record and raw_record[0] == b'\x00'[0]:  # Not deleted
                        record = TypeConverter.bytes_to_values(
                            raw_record,
                            table_info["format_str"],
                            table_info["columns"]
                        )
                        records.append(record)
            
            return records
    
    def _select_index_type(self, table_info: Dict[str, Any], column: str, filter: Dict[str, Any]) -> str:
        """Select the appropriate index type based on the rules"""
        logger.debug(f"Selecting index type for column {column}")
        logger.debug(f"Available indexes: {table_info['indexes']}")
        
        # If column is primary key and no explicit index requested, use B+ tree
        if column == table_info.get("primary_key") and not filter.get("requested_index"):
            logger.debug(f"Using B+ tree index for primary key {column}")
            return "bplus"
            
        # If explicit index type requested, use it if available
        if filter.get("requested_index") and table_info["indexes"].get(column) == filter["requested_index"]:
            logger.debug(f"Using explicitly requested index type {filter['requested_index']} for {column}")
            return filter["requested_index"]
            
        # For indexed attributes, follow the priority order
        if column in table_info["indexes"]:
            index_type = table_info["indexes"][column]
            if index_type == "bplus":
                return "bplus"
            elif index_type == "hash":
                return "hash"
            elif index_type == "sequential":
                return "sequential"
            elif index_type == "isam":
                return "isam"
            elif index_type == "rtree":
                return "rtree"
                
        return None

    def _get_filtered_records(self, cursor: LineCursor, table_info: Dict[str, Any], filter: Dict[str, Any]) -> List[List[Any]]:
        """Get records applying filter without using an index"""
        records = []
        col = filter["column"]
        operation = filter["operation"]
        
        # Get column info
        col_idx = next(
            (i for i, c in enumerate(table_info["columns"]) if c["name"] == col),
            -1
        )
        if col_idx == -1:
            return []
            
        col_type = table_info["columns"][col_idx]["type"]
        
        with cursor as c:
            total = c.total_records()
            
            for i in range(total):
                c.goto_record(i)
                raw_record = c.read_record()
                if raw_record:
                    record = TypeConverter.bytes_to_values(
                        raw_record,
                        table_info["format_str"],
                        table_info["columns"]
                    )
                    
                    # Apply filter
                    if operation == "=":
                        if str(record[col_idx]) == str(filter["value"]):
                            records.append(record)
                    elif operation == "BETWEEN":
                        from_val = filter["from"].strip('"')
                        to_val = filter["to"].strip('"')
                        if from_val <= str(record[col_idx]) <= to_val:
                            records.append(record)
                            
        return records
