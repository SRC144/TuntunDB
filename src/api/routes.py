from flask import Blueprint, request, jsonify, current_app
from db.engine.query_handler import QueryHandler
from .utils import CustomJSONEncoder
import json

api_bp = Blueprint('api', __name__)

# Create a query handler instance
handler = QueryHandler()

def json_response(data, status=200):
    """Helper function to create JSON responses with proper encoding"""
    return current_app.response_class(
        json.dumps(data, cls=CustomJSONEncoder, indent=2),
        mimetype='application/json',
        status=status
    )

@api_bp.route('/query', methods=['POST'])
def execute_query():
    """Execute a SQL query"""
    if not request.is_json:
        return json_response({'error': 'Content-Type must be application/json'}, status=400)
        
    data = request.get_json()
    
    if 'query' not in data:
        return json_response({'error': 'Query is required'}, status=400)
        
    query = data['query']
    
    try:
        result = handler.execute_query(query)
        return json_response(result)
    except Exception as e:
        return json_response({'error': str(e)}, status=400)

@api_bp.route('/batch', methods=['POST'])
def execute_batch():
    """Execute multiple SQL queries in a batch"""
    if not request.is_json:
        return json_response({'error': 'Content-Type must be application/json'}, status=400)
        
    data = request.get_json()
    
    if 'queries' not in data or not isinstance(data['queries'], list):
        return json_response({'error': 'Queries list is required'}, status=400)
        
    queries = data['queries']
    results = []
    
    for query in queries:
        try:
            result = handler.execute_query(query)
            results.append({
                'query': query,
                'result': result,
                'status': 'success'
            })
        except Exception as e:
            results.append({
                'query': query,
                'error': str(e),
                'status': 'error'
            })
            
    return json_response({'results': results})

# Example queries for testing
example_queries = [
    """CREATE TABLE Restaurantes (
        id INT KEY INDEX BPlusTree,
        nombre VARCHAR[20] INDEX BPlusTree,
        fechaRegistro DATE,
        ubicacion ARRAY[FLOAT]
    );""",
    "INSERT INTO Restaurantes VALUES (1, 'El Buen Sabor', '2024-01-01', '10.5,20.3');"
] 