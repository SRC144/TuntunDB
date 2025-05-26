from flask import Blueprint, request, jsonify, current_app
from db.engine.query_handler import QueryHandler
from .utils import CustomJSONEncoder
import json
import os

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

@api_bp.route('/tables', methods=['GET'])
def get_tables():
    """Get list of available tables"""
    try:
        tables = handler.table_manager.get_all_tables()
        return json_response({
            "status": "success",
            "tables": tables
        })
    except Exception as e:
        return json_response({
            "status": "error",
            "message": str(e)
        }, status=400)

@api_bp.route('/query', methods=['POST'])
def execute_query():
    """Execute a SQL query"""
    if not request.is_json:
        print("[DEBUG] Request is not JSON")
        return json_response({'error': 'Content-Type must be application/json'}, status=400)
        
    data = request.get_json()
    print(f"[DEBUG] Received query request: {data}")
    
    if 'query' not in data:
        print("[DEBUG] No query in request")
        return json_response({'error': 'Query is required'}, status=400)
        
    query = data['query']
    print(f"[DEBUG] Processing query: {query}")
    
    try:
        result = handler.execute_query(query)
        print(f"[DEBUG] Query result: {result}")
        # If result contains an error, return it with status 400
        if isinstance(result, dict) and 'error' in result:
            print(f"[DEBUG] Query error: {result['error']}")
            return json_response(result, status=400)
        print("[DEBUG] Query successful")
        return json_response(result)
    except Exception as e:
        print(f"[DEBUG] Query exception: {str(e)}")
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
    """INSERT INTO Restaurantes VALUES (1, "El Buen Sabor", "2024-01-01", "10.5,20.3");""",
    """INSERT INTO Restaurantes VALUES (2, "La Pizzeria", "2024-01-02", "15.7,25.1");""",
    """SELECT * FROM Restaurantes WHERE nombre BETWEEN "A" AND "M";""",
    """DELETE FROM Restaurantes WHERE id = 1;""",
    """DELETE FROM Restaurantes WHERE nombre = "La Pizzeria";"""
] 