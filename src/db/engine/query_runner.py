from typing import Dict, Any
from ..commands.create import CreateCommand
from ..commands.insert import InsertCommand
from ..commands.select import SelectCommand
from ..commands.delete import DeleteCommand
from ..commands.update import UpdateCommand
from ..commands.drop import DropCommand
from ..storage_management.table_manager import TableManager

class QueryRunner:
    def __init__(self, table_manager: TableManager):
        self.table_manager = table_manager
        # Initialize all command handlers
        self.commands = {
            'CREATE': CreateCommand(table_manager),
            'INSERT': InsertCommand(table_manager),
            'SELECT': SelectCommand(table_manager),
            'DELETE': DeleteCommand(table_manager),
            'UPDATE': UpdateCommand(table_manager),
            'DROP': DropCommand(table_manager)
        }
    
    def execute(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a parsed query using the appropriate command handler"""
        command_type = parsed_query.get('type', '').upper()
        
        if command_type not in self.commands:
            return {"error": f"Unsupported command type: {command_type}"}
            
        try:
            command = self.commands[command_type]
            return command.execute(parsed_query)
        except Exception as e:
            return {"error": str(e)}
