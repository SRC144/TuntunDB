from typing import Dict, Any
from ..commands.select import SelectCommand
from ..commands.insert import InsertCommand
from ..storage_management.table_manager import TableManager

class QueryRunner:
    def __init__(self, table_manager: TableManager):
        self.table_manager = table_manager
        self.command_map = {
            "SELECT": SelectCommand,
            "INSERT": InsertCommand
        }
    
    def execute(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a parsed query"""
        action = parsed_query["action"]
        table_name = parsed_query["table_name"]
        
        # Special handling for CREATE queries
        if action == "CREATE":
            return self.table_manager.create_table(
                table_name,
                parsed_query["columns"],
                parsed_query["indexes"],
                parsed_query.get("primary_key")
            )
        elif action == "CREATE_FROM_FILE":
            return self.table_manager.create_table_from_file(
                table_name,
                parsed_query["file_path"],
                parsed_query["index"]
            )
            
        # For other queries, get table info and execute appropriate command
        table_info = self.table_manager.get_table_info(table_name)
        if not table_info:
            return {"error": f"Table {table_name} does not exist"}
            
        command_class = self.command_map.get(action)
        if not command_class:
            return {"error": f"Unsupported action: {action}"}
            
        command = command_class(table_info)
        return command.execute(parsed_query)
