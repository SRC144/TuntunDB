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
                return {"error": parsed_query["error"]}
            
            # Ejecutar query usando el runner
            result = self.runner.execute(parsed_query)
            return result
            
        except Exception as e:
            return {"error": str(e)}
