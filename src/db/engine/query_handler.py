from .query_parser import QueryParser
from .query_runner import QueryRunner
from ..storage_management.table_manager import TableManager

class QueryHandler:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.parser = QueryParser()
        self.table_manager = TableManager(data_dir)
        self.runner = QueryRunner(self.table_manager)
    
    def execute_query(self, query: str):
        """
        1. Parseamos query
        2. Obtenemos informacion de la tabla
        3. Ejecutamos la query creando el comando correspondiente
        """
        try:
            # Parsear query a formato estructurado
            parsed_query = self.parser.parse(query)
            
            if "error" in parsed_query:
                return {
                    "status": "error",
                    "message": parsed_query["error"]
                }
            
            # Ejecutar query usando el runner
            result = self.runner.execute(parsed_query)
            
            # For SELECT queries, return the records directly
            if parsed_query["type"] == "SELECT" and isinstance(result, dict) and "records" in result:
                return {
                    "status": "success",
                    "records": result["records"],
                    "columns": result.get("columns", []),
                    "table_name": parsed_query["table_name"]
                }
            
            # For other queries (CREATE, INSERT, DELETE), return a single success message
            if isinstance(result, dict):
                if "error" in result or ("message" in result and "already exists" in result["message"]):
                    return {
                        "status": "error",
                        "message": result.get("error", result["message"])
                    }
                elif "message" in result:
                    return {
                        "status": "success",
                        "message": result["message"]
                    }
                else:
                    return {
                        "status": "success",
                        "message": "Operation completed successfully"
                    }
            else:
                return {
                    "status": "success",
                    "message": "Operation completed successfully"
                }
            
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }
