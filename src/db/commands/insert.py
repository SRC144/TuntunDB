from typing import Dict, Any, List
from ..storage_management.table_manager import TableManager
from ..index_handling.index_factory import IndexFactory
from ..utils.type_converter import TypeConverter
from ..cursors.line_cursor import LineCursor
import struct

class InsertCommand:
    def __init__(self, table_manager: TableManager):
        self.table_manager = table_manager
    
    def execute(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute INSERT command"""
        print("[DEBUG] Starting INSERT command execution")
        table_name = parsed_query["table_name"]
        values = parsed_query["values"]
        print(f"[DEBUG] Table: {table_name}, Values: {values}")
        
        # Get table info
        table_info = self.table_manager.get_table_info(table_name)
        if not table_info:
            return {
                "status": "error",
                "message": f"Table {table_name} not found"
            }
            
        # Convert values to binary record
        print("[DEBUG] Converting values to binary record")
        try:
            record = TypeConverter.convert_record(
                values=values,
                columns=table_info["columns"],
                format_str=table_info["format_str"]
            )
        except Exception as e:
            return {
                "status": "error",
                "message": f"Invalid values: {str(e)}"
            }
            
        # Check primary key constraint
        primary_key = table_info.get("primary_key")
        if primary_key:
            print(f"[DEBUG] Checking primary key constraint for column: {primary_key}")
            # Get primary key value and position
            pk_idx = next(
                (i for i, c in enumerate(table_info["columns"]) if c["name"] == primary_key),
                -1
            )
            if pk_idx == -1:
                return {
                    "status": "error",
                    "message": f"Primary key column {primary_key} not found"
                }

            pk_value = values[pk_idx]
            pk_type = table_info["columns"][pk_idx]["type"]
            print(f"[DEBUG] Primary key value: {pk_value}, type: {pk_type}")

            # Primary key should always have a B+ tree index
            if primary_key not in table_info["indexes"] or table_info["indexes"][primary_key].lower() != 'bplus':
                print(f"[DEBUG] No B+ tree index found for primary key {primary_key}")
                return {
                    "status": "error",
                    "message": f"Primary key {primary_key} must have a B+ tree index"
                }

            print("[DEBUG] Using index to check primary key uniqueness")
            # Use index to check if key exists
            index = IndexFactory.get_index(
                index_type='bplus',
                index_filename=table_info["index_files"][primary_key],
                data_filename=table_info["data_file"],
                data_format=table_info["format_str"],
                key_position=pk_idx + 1  # +1 to account for deletion marker
            )
            
            # Convert value to appropriate type for search
            search_key = TypeConverter.convert_value(pk_value, pk_type)
            print(f"[DEBUG] Searching for key: {search_key}")
            
            # Convert search key to bytes for B+ tree search
            if isinstance(search_key, int):
                search_key = search_key.to_bytes(8, byteorder='little')
            elif isinstance(search_key, str):
                search_key = search_key.encode().ljust(8, b'\x00')
            elif isinstance(search_key, bytes):
                search_key = search_key[:8].ljust(8, b'\x00')
            
            print(f"[DEBUG] Searching for key bytes: {search_key}")
            # Search in index - if we find ANY record, it's a duplicate
            result = index.search(search_key)
            if result is not None:
                # Check if the found record is not deleted
                cursor = LineCursor(table_info["data_file"], struct.calcsize(table_info["format_str"]))
                with cursor as c:
                    c.goto_record(result)
                    found_record = c.read_record()
                    if found_record and found_record[0] == b'\x00'[0]:  # Not deleted
                        print(f"[DEBUG] Primary key {pk_value} already exists")
                        return {
                            "status": "error",
                            "message": f"Record with {primary_key}={pk_value} already exists"
                        }

        # Write record to data file and update indexes
        print("[DEBUG] Writing record and updating indexes")
        try:
            # Write record to data file using primary key index
            print(f"[DEBUG] Using primary key index to write record")
            primary_index = IndexFactory.get_index(
                index_type='bplus',
                index_filename=table_info["index_files"][primary_key],
                data_filename=table_info["data_file"],
                data_format=table_info["format_str"],
                key_position=pk_idx + 1  # +1 to account for deletion marker
            )
            primary_index.add(record)
            print(f"[DEBUG] Record written using primary key index")
            
            # Update remaining indexes (excluding primary key)
            remaining_indexes = {k: v for k, v in table_info["indexes"].items() if k != primary_key}
            for col, index_type in remaining_indexes.items():
                print(f"[DEBUG] Processing index for column {col} of type {index_type}")
                col_idx = next(
                    (i for i, c in enumerate(table_info["columns"]) if c["name"] == col),
                    -1
                )
                if col_idx == -1:
                    raise Exception(f"Column {col} not found in table schema")

                # Create appropriate index instance based on type
                index = IndexFactory.get_index(
                    index_type=index_type.lower(),  # Ensure lowercase for consistency
                    index_filename=table_info["index_files"][col],
                    data_filename=table_info["data_file"],
                    data_format=table_info["format_str"],
                    key_position=col_idx + 1  # +1 to account for deletion marker
                )
                
                # Add record to index
                print(f"[DEBUG] Adding record to {index_type} index for column {col}")
                try:
                    index.add(record)
                    print(f"[DEBUG] Successfully added record to {index_type} index for {col}")
                except Exception as e:
                    print(f"[DEBUG] Error adding record to {index_type} index for {col}: {str(e)}")
                    raise Exception(f"Failed to update {index_type} index for column {col}: {str(e)}")

            # Update table stats
            self.table_manager.update_table_stats(table_name, total_delta=1)

            print("[DEBUG] Insert completed successfully")
            return {
                "status": "success",
                "message": "Record inserted successfully"
            }
        except Exception as e:
            print(f"[DEBUG] Error updating indexes: {str(e)}")
            return {
                "status": "error",
                "message": f"Failed to update indexes: {str(e)}"
            }

    def _update_indexes(self, table_info: Dict[str, Any], record: bytes) -> None:
        """Update all indexes for the table with the new record"""
        print(f"[DEBUG] Starting index updates. Table info: {table_info}")  # DEBUG
        for col, index_file in table_info["index_files"].items():
            print(f"[DEBUG] Processing index for column {col}")  # DEBUG
            try:
                # Get column position
                col_idx = next(
                    (i for i, c in enumerate(table_info["columns"]) if c["name"] == col),
                    -1
                )
                print(f"[DEBUG] Column {col} found at position {col_idx}")  # DEBUG
                
                if col_idx == -1:
                    raise Exception(f"Column {col} not found in table schema")

                index = IndexFactory.get_index(
                    index_type='bplus',  # Currently hardcoded as we only support B+ trees
                    index_filename=index_file,
                    data_filename=table_info["data_file"],
                    data_format=table_info["format_str"],
                    key_position=col_idx
                )
                
                print(f"[DEBUG] Created index for {col}, adding record")  # DEBUG
                # Add record to index
                index.add(record)
                print(f"[DEBUG] Successfully added record to index for {col}")  # DEBUG
            except Exception as e:
                print(f"[DEBUG] Error updating index for {col}: {str(e)}")  # DEBUG
                raise Exception(f"Failed to update index for column {col}: {str(e)}") 