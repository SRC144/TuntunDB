from typing import Dict, Any, List
from ..index_handling.index_factory import IndexFactory
from ..utils.type_converter import TypeConverter
from ..storage_management.table_manager import TableManager
from ..storage_management.compaction import TableCompactor
import struct
from ..cursors.line_cursor import LineCursor
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class DeleteCommand:
    def __init__(self, table_manager: TableManager):
        self.table_manager = table_manager
        self.compactor = TableCompactor(table_manager)
    
    def execute(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a DELETE query"""
        logger.debug("Starting DELETE command execution")
        table_name = parsed_query["table_name"]
        
        # Get table info
        table_info = self.table_manager.get_table_info(table_name)
        if not table_info:
            logger.error(f"Table {table_name} not found")
            return {"status": "error", "message": f"Table {table_name} not found"}

        if "filters" not in parsed_query or not parsed_query["filters"]:
            logger.debug("No filters found in DELETE query")
            return {"status": "error", "message": "DELETE requires WHERE clause"}
            
        filter = parsed_query["filters"][0]  # For now, handle only first filter
        if filter["operation"] != "=":
            logger.debug(f"Unsupported operation: {filter['operation']}")
            return {"status": "error", "message": "Only equality conditions are supported for DELETE"}

        # Get column info for the filter
        col = filter["column"]
        col_idx = next(
            (i for i, c in enumerate(table_info["columns"]) if c["name"] == col),
            -1
        )
        if col_idx == -1:
            logger.error(f"Column {col} not found in table schema")
            return {"status": "error", "message": f"Column {col} not found"}
            
        col_type = table_info["columns"][col_idx]["type"]
        logger.debug(f"Using filter on column: {col} (type: {col_type})")

        # Convert search value to appropriate type
        try:
            search_value = TypeConverter.convert_value(filter["value"], col_type)
            logger.debug(f"Converted search value {filter['value']} to {search_value}")
        except Exception as e:
            logger.error(f"Error converting search value: {str(e)}")
            return {"status": "error", "message": f"Invalid search value: {str(e)}"}

        # First locate the record using the best available index
        record_pos = None
        if col in table_info["indexes"]:
            index_type = table_info["indexes"][col]
            logger.debug(f"Using {index_type} index for search on column {col}")
            
            index = IndexFactory.get_index(
                index_type=index_type.lower(),
                index_filename=table_info["index_files"][col],
                data_filename=table_info["data_file"],
                data_format=table_info["format_str"],
                key_position=col_idx + 1  # +1 to account for deletion marker
            )
            
            # Prepare search key based on index type
            if index_type.lower() == 'bplus':
                if isinstance(search_value, int):
                    search_key = search_value.to_bytes(8, byteorder='little')
                elif isinstance(search_value, str):
                    search_key = search_value.encode().ljust(8, b'\x00')
                elif isinstance(search_value, float):
                    search_key = struct.pack('=d', search_value)
                else:
                    search_key = search_value
                logger.debug(f"Prepared search key bytes: {search_key}")
            else:
                search_key = search_value
            
            record_pos = index.search(search_key)
            logger.debug(f"Index search result: {record_pos}")
        else:
            logger.error(f"No index available for column {col}")
            return {"status": "error", "message": f"DELETE requires an index on column {col}"}

        if record_pos is None:
            logger.debug("Record not found")
            return {
                "status": "error",
                "message": f"Record with {col}={filter['value']} not found"
            }

        # Read the record to get all field values
        record_size = struct.calcsize(table_info["format_str"])
        cursor = LineCursor(table_info["data_file"], record_size)
        
        try:
            with cursor as c:
                # Read the record
                c.goto_record(record_pos)
                record = c.read_record()
                if not record:
                    logger.error("Could not read record")
                    return {"status": "error", "message": "Could not read record"}
                
                # Extract all field values from the record
                record_values = struct.unpack(table_info["format_str"], record)
                
                # Check if already deleted (first byte is deletion marker)
                if record_values[0] == b'\x01':  # Marker for deleted
                    logger.debug("Record already marked as deleted")
                    return {
                        "status": "success",
                        "message": f"Record with {col}={filter['value']} was already deleted"
                    }
                
                # Mark record as deleted by setting deletion marker
                new_record = b'\x01' + record[1:]  # Set deletion marker to 1
                c.overwrite_current(new_record)
                
                # Update table stats
                self.table_manager.update_table_stats(table_name, deleted_delta=1)
                
                # Check if table should be compacted
                if self.table_manager.should_compact(table_name):
                    logger.debug("Table needs compaction")
                    compaction_result = self.compactor.compact_table(table_name)
                    if compaction_result.get("status") == "error":
                        logger.error(f"Compaction failed: {compaction_result.get('message')}")
                
                return {
                    "status": "success",
                    "message": f"Record with {col}={filter['value']} deleted successfully"
                }
                
        except Exception as e:
            logger.error(f"Error during delete: {str(e)}")
            return {
                "status": "error",
                "message": f"Failed to delete record: {str(e)}"
            } 