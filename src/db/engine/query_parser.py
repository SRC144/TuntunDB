import re
import sqlglot
from sqlglot import parse_one, exp
from typing import Dict, Any, List

class QueryParser:
    def __init__(self):
        self.supported_types = {
            'INT', 'VARCHAR', 'DATE', 'FLOAT', 'ARRAY'
        }
        self.supported_indexes = {
            'BPLUS', 'SEQUENTIAL', 'HASH', 'ISAM', 'RTREE'
        }
    
    def parse(self, query: str) -> Dict[str, Any]:
        """Entrada principal del parser"""
        query = query.strip()
        query_upper = query.upper()
        
        if query_upper.startswith("CREATE TABLE"):
            if "FROM FILE" in query_upper:
                return self._parse_create_from_file(query)
            return self._parse_create_table(query)
        
        if query_upper.startswith("DROP TABLE"):
            return self._parse_drop_table(query)
            
        # Manejo especial para INSERT con VALUES
        if query_upper.startswith("INSERT"):
            # Limpiar la query eliminando nuevas lineas y espacios extra
            query = ' '.join(line.strip() for line in query.split('\n'))
            # Eliminar cualquier parentesis alrededor de la clausula VALUES
            query = re.sub(r'VALUES\s*\((.*)\)', r'VALUES \1', query)
        
        try:
            ast = parse_one(query)
            if isinstance(ast, exp.Select):
                return self._parse_select(ast)
            elif isinstance(ast, exp.Insert):
                return self._parse_insert(ast)
            elif isinstance(ast, exp.Delete):
                return self._parse_delete(ast)
            else:
                return {"error": "Unsupported query type"}
        except Exception as e:
            return {"error": f"Parse error: {str(e)}"}
    
    def _parse_create_table(self, query: str) -> Dict[str, Any]:
        """Parse CREATE TABLE query con schema definition"""
        # Eliminar nuevas lineas y espacios extra
        query = ' '.join(query.split())
        
        match = re.search(r'CREATE TABLE (\w+)\s*\((.*?)\);?', query, re.IGNORECASE | re.DOTALL)
        if not match:
            return {"error": "Malformed CREATE TABLE"}

        table_name = match.group(1).lower()  # Convert to lowercase
        body = match.group(2)
        column_defs = [s.strip() for s in body.split(",")]

        columns = []
        indexes = {}
        primary_key = None

        for col_def in column_defs:
            parts = col_def.split()
            if len(parts) < 2:
                continue
                
            col_name = parts[0]
            col_type = parts[1]
            
            # Parse array type
            array_match = re.search(r'ARRAY\[(.*?)\]', col_type)
            if array_match:
                base_type = array_match.group(1)
                if base_type not in self.supported_types:
                    return {"error": f"Unsupported array type: {base_type}"}
                col_type = f"ARRAY[{base_type}]"
            elif not (col_type in self.supported_types or re.match(r'VARCHAR\[\d+\]', col_type)):
                # Check if it's VARCHAR with size
                varchar_match = re.search(r'VARCHAR\[(\d+)\]', col_type)
                if not varchar_match:
                    return {"error": f"Unsupported type: {col_type}"}
            
            # Check for KEY (primary key)
            if 'KEY' in parts:
                primary_key = col_name
                # Primary key automatically gets a B+ tree index
                indexes[col_name] = 'bplus'
            
            # Check for INDEX
            index_match = re.search(r'INDEX\s+(\w+)', col_def, re.IGNORECASE)
            if index_match:
                index_type = index_match.group(1).lower()
                if index_type.upper() in self.supported_indexes:
                    indexes[col_name] = index_type

            columns.append({"name": col_name, "type": col_type})

        return {
            "type": "CREATE",
            "table_name": table_name,
            "columns": columns,
            "indexes": indexes,
            "primary_key": primary_key
        }
    
    def _parse_create_from_file(self, query: str) -> Dict[str, Any]:
        """Parse CREATE TABLE from file with index specifications"""
        # Basic pattern for table name and file path
        base_pattern = r'create table (\w+) from file ["\']([^"\']+)["\']'
        base_match = re.search(base_pattern, query, re.IGNORECASE)
        
        if not base_match:
            return {"error": "Invalid CREATE TABLE FROM FILE syntax"}
            
        table_name, file_path = base_match.groups()
        table_name = table_name.lower()  # Convert to lowercase
        
        # Initialize empty index info
        index_info = {}
        
        # Check for index specifications
        index_pattern = r'using index (\w+)\(["\']?(\w+)["\']?\)'
        index_matches = re.finditer(index_pattern, query, re.IGNORECASE)
        
        for match in index_matches:
            index_type, index_column = match.groups()
            index_type = index_type.lower()
            if index_type not in [t.lower() for t in self.supported_indexes]:
                continue  # Skip unsupported index types
            index_info[index_column] = index_type
            
        return {
            "type": "CREATE",
            "from_file": True,
            "table_name": table_name,
            "file_path": file_path,
            "index_info": index_info
        }
    
    def _parse_select(self, node: exp.Select) -> Dict[str, Any]:
        """Parse SELECT query"""
        from_clause = node.args.get('from')
        if not from_clause:
            return {"error": "Missing FROM clause"}

        table_name = None
        if hasattr(from_clause, 'expressions') and from_clause.expressions:
            table_name = from_clause.expressions[0].this.sql().lower()  # Convert to lowercase
        elif hasattr(from_clause, 'this'):
            table_name = from_clause.this.sql().lower()  # Convert to lowercase

        selected_columns = [col.sql() for col in node.expressions]
        filters = []

        if node.args.get("where"):
            filters = self._parse_where(node.args["where"])
            
        # Check for USING INDEX hint in comments
        requested_index = None
        if node.comments:
            for comment in node.comments:
                if "USING INDEX" in comment.upper():
                    # Extract index type from comment (e.g. "USING INDEX BPLUS" -> "bplus")
                    parts = comment.upper().split()
                    if len(parts) > 2:
                        requested_index = parts[2].lower()

        return {
            "type": "SELECT",
            "table_name": table_name,
            "columns": selected_columns,
            "filters": filters,
            "requested_index": requested_index
        }
    
    def _parse_where(self, where_clause: exp.Expression) -> List[Dict[str, Any]]:
        """Parse WHERE clause conditions"""
        filters = []

        if isinstance(where_clause, exp.Where):
            # If it's a Where node, get the actual condition
            where_clause = where_clause.this

        if isinstance(where_clause, exp.EQ):
            column = where_clause.this.sql() if isinstance(where_clause.this, exp.Column) else where_clause.left.sql()
            value = where_clause.expression.sql() if hasattr(where_clause, 'expression') else where_clause.right.sql()
            filters.append({
                "column": column,
                "operation": "=",
                "value": value.strip("'\"")  # Remove quotes from string values
            })

        elif isinstance(where_clause, exp.Between):
            filters.append({
                "column": where_clause.this.sql(),
                "operation": "BETWEEN",
                "from": where_clause.args['low'].sql(),
                "to": where_clause.args['high'].sql()
            })

        #maybe no flexible
        elif isinstance(where_clause, exp.In):
            column = where_clause.this.sql()
            expressions = where_clause.expressions
            if len(expressions) == 2:
                point_exp = expressions[0].sql()
                radius_exp = expressions[1].sql()
                point_match = re.search(r'POINT\(([\d\., ]+)\)', point_exp)
                if point_match:
                    coords = [float(c.strip()) for c in point_match.group(1).split(',')]
                    radius = float(radius_exp)
                    filters.append({
                        "column": column,
                        "type": "spatial",
                        "operation": "in_radius",
                        "point": coords,
                        "radius": radius
                    })
                else:
                    filters.append({"error": "Invalid POINT syntax"})
            else:
                filters.append({"error": "Unsupported IN clause"})

        return filters
    
    def _parse_insert(self, node: exp.Insert) -> Dict[str, Any]:
        """Parse INSERT query"""
        
        # Get table name and convert to lowercase
        table_name = node.this.this.sql().strip("'\"").lower()
        
        # Get values from the VALUES clause
        values = []
        if node.args.get('expression') and hasattr(node.args['expression'], 'expressions'):
            for tuple_expr in node.args['expression'].expressions:
                if hasattr(tuple_expr, 'expressions'):
                    # Each tuple contains one value
                    val = tuple_expr.expressions[0].sql().strip("'\"")
                    # Handle ARRAY constructor
                    if val.upper().startswith('ARRAY['):
                        val = val[6:-1]  # Remove ARRAY[ and ]
                    # Remove any extra whitespace and quotes
                    val = val.strip().strip("'\"")
                    values.append(val)
            
        return {
            "type": "INSERT",
            "table_name": table_name,
            "values": values
        }
    
    def _parse_delete(self, node: exp.Delete) -> Dict[str, Any]:
        """Parse DELETE query"""
        
        # Check for WHERE clause
        if not node.args.get("where"):
            return {"error": "DELETE requires WHERE clause"}
            
        # Convert table name to lowercase
        table_name = node.this.this.sql().strip("'\"").lower()  # Convert to lowercase
        
        filters = self._parse_where(node.args["where"])
        
        return {
            "type": "DELETE",
            "table_name": table_name,
            "filters": filters
        }

    def _parse_drop_table(self, query: str) -> Dict[str, Any]:
        """Parse DROP TABLE query"""
        match = re.search(r'DROP TABLE (\w+);?', query, re.IGNORECASE)
        if not match:
            return {"error": "Invalid DROP TABLE syntax"}
            
        table_name = match.group(1).lower()  # Convert to lowercase
        return {
            "type": "DROP",
            "table_name": table_name
        }
