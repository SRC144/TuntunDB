from typing import Dict, Any
import os
import shutil
import logging
from ..storage_management.table_manager import TableManager

logger = logging.getLogger(__name__)

class DropCommand:
    def __init__(self, table_manager: TableManager):
        self.table_manager = table_manager
    
    def execute(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute DROP TABLE command"""
        
        # Get and validate table name
        table_name = parsed_query.get("table_name", "").lower()
        if not table_name:
            return {
                "status": "error",
                "message": "No table name provided"
            }
            
        # Check if table exists
        if not self.table_manager.table_exists(table_name):
            return {
                "status": "error",
                "message": f"Table {table_name} does not exist"
            }
            
        try:
            # Get table info to ensure all files are known
            table_info = self.table_manager.get_table_info(table_name)
            if not table_info:
                return {
                    "status": "error",
                    "message": f"Could not read table info for {table_name}"
                }
                
            # Get table directory
            table_dir = os.path.join(self.table_manager.data_dir, table_name)
            
            # Validate path is within data directory
            table_dir = os.path.abspath(table_dir)
            data_dir = os.path.abspath(self.table_manager.data_dir)
            if not table_dir.startswith(data_dir + os.sep):
                return {
                    "status": "error",
                    "message": "Invalid table name: path traversal detected"
                }
            
            # Remove the entire table directory
            if os.path.exists(table_dir):
                shutil.rmtree(table_dir)
                logger.debug(f"Successfully dropped table {table_name}")
                return {
                    "status": "success",
                    "message": f"Table {table_name} dropped successfully"
                }
            else:
                return {
                    "status": "error",
                    "message": f"Table directory not found for {table_name}"
                }
                
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {str(e)}")
            return {
                "status": "error",
                "message": f"Failed to drop table: {str(e)}"
            } 