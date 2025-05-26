from typing import Dict, Any
import struct
import os
import time
from ..cursors.line_cursor import LineCursor
from ..storage_management.table_manager import TableManager
from ..index_handling.index_factory import IndexFactory
import logging

logger = logging.getLogger(__name__)

class TableCompactor:
    def __init__(self, table_manager: TableManager):
        self.table_manager = table_manager

    def compact_table(self, table_name: str) -> Dict[str, Any]:
        """Compact a table by removing deleted records and rebuilding indexes"""
        
        table_info = self.table_manager.get_table_info(table_name)
        if not table_info:
            return {"status": "error", "message": f"Table {table_name} not found"}

        try:
            # Create temporary data file
            temp_data_file = f"{table_info['data_file']}.temp"
            record_size = struct.calcsize(table_info["format_str"])
            
            # Open source cursor
            src_cursor = LineCursor(table_info["data_file"], record_size)
            
            # Track new positions for rebuilding indexes
            new_positions = {}  # old_pos -> new_pos mapping
            new_record_count = 0
            
            # Create temporary index files
            temp_indexes = {}
            for col, index_type in table_info["indexes"].items():
                idx_file = table_info["index_files"][col]
                temp_idx_file = f"{idx_file}.temp"
                temp_indexes[col] = temp_idx_file
            
            # Copy non-deleted records to temp file
            with open(temp_data_file, 'wb') as dest_file:
                with src_cursor as cursor:
                    total_records = cursor.total_records()
                    for i in range(total_records):
                        cursor.goto_record(i)
                        record = cursor.read_record()
                        if not record:
                            continue
                            
                        # Check if record is not deleted
                        if record[0] == b'\x00'[0]:  # Not deleted
                            new_pos = dest_file.tell()
                            new_positions[i] = new_pos
                            dest_file.write(record)
                            new_record_count += 1

            # Rebuild indexes
            for col, index_type in table_info["indexes"].items():
                # Get column position (offset by 1 for deletion marker)
                col_idx = next(
                    (i for i, c in enumerate(table_info["columns"]) if c["name"] == col),
                    -1
                )
                
                if col_idx == -1:
                    continue
                    
                # Create new index instance
                new_index = IndexFactory.get_index(
                    index_type=index_type.lower(),
                    index_filename=temp_indexes[col],
                    data_filename=temp_data_file,
                    data_format=table_info["format_str"],
                    key_position=col_idx + 1  # +1 to account for deletion marker
                )
                
                # Rebuild index
                with LineCursor(temp_data_file, record_size) as cursor:
                    while not cursor.eof():
                        record = cursor.read_record()
                        values = struct.unpack(table_info["format_str"], record)
                        print(values)
                        key = values[col_idx + 1]  # +1 to skip deletion marker
                        new_index.add(key)
                        cursor.advance_record()

            # Update table info
            table_info["stats"]["total_records"] = new_record_count
            table_info["stats"]["deleted_records"] = 0
            table_info["stats"]["last_compaction"] = int(time.time())
            
            # Replace old files with new ones
            os.replace(temp_data_file, table_info["data_file"])
            for col, temp_idx_file in temp_indexes.items():
                os.replace(temp_idx_file, table_info["index_files"][col])
                
            # Save updated table info
            self.table_manager._save_table_info(table_name, table_info)
            
            return {
                "status": "success",
                "message": f"Table compacted: {new_record_count} records retained"
            }
            
        except Exception as e:
            logger.error(f"Error during compaction: {str(e)}")
            # Cleanup any temporary files
            if os.path.exists(temp_data_file):
                os.remove(temp_data_file)
            for temp_idx_file in temp_indexes.values():
                if os.path.exists(temp_idx_file):
                    os.remove(temp_idx_file)
                    
            return {
                "status": "error",
                "message": f"Compaction failed: {str(e)}"
            } 