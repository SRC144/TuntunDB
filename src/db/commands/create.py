from typing import Dict, Any, List
import csv
import os
from ..storage_management.table_manager import TableManager
from ..index_handling.index_factory import IndexFactory
from ..utils.type_converter import TypeConverter

class CreateCommand:
    def __init__(self, table_manager: TableManager):
        self.table_manager = table_manager

    def execute(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute CREATE TABLE command, either from schema or from file
        """
        if parsed_query.get("from_file"):
            return self._create_from_file(parsed_query)
        else:
            return self._create_table(parsed_query)

    def _create_table(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new table from schema definition"""
        print("[DEBUG] Starting _create_table")  # DEBUG
        table_name = parsed_query["table_name"]
        columns = parsed_query["columns"]
        indexes = parsed_query.get("indexes", {})
        primary_key = parsed_query.get("primary_key")

        # If there's a primary key, ensure it has a B+ tree index
        if primary_key:
            print(f"[DEBUG] Adding B+ tree index for primary key column: {primary_key}")
            indexes[primary_key] = 'bplus'

        # Create the table structure first
        print(f"[DEBUG] Calling create_table for {table_name}")  # DEBUG
        table_info = self.table_manager.create_table(
            table_name=table_name,
            columns=columns,
            indexes=indexes,
            primary_key=primary_key
        )
        print(f"[DEBUG] create_table returned: {table_info}")  # DEBUG

        # Check for table creation errors
        if table_info.get("status") == "error":
            print(f"[DEBUG] Returning error from create_table: {table_info}")  # DEBUG
            return table_info

        # Initialize indexes
        print("[DEBUG] Initializing indexes")  # DEBUG
        index_result = self._initialize_indexes(table_name)
        print(f"[DEBUG] Index initialization returned: {index_result}")
        if index_result.get("status") == "error":
            print(f"[DEBUG] Returning error from index init: {index_result}")  # DEBUG
            return index_result
        
        print("[DEBUG] Returning final success")  # DEBUG
        return {
            "status": "success",
            "message": f"Table {table_name} created successfully with all specified indexes"
        }

    def _create_from_file(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new table from a CSV file"""
        table_name = parsed_query["table_name"]
        file_path = parsed_query["file_path"]
        index_info = parsed_query.get("index_info", {})
        
        try:
            # Read CSV headers and first row to infer schema
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                headers = next(reader)
                first_row = next(reader)

            # Infer column types
            columns = []
            primary_key = None
            for header, value in zip(headers, first_row):
                col_type = self._infer_type(value)
                # If this is the first column, make it the primary key
                if not primary_key:
                    primary_key = header
                columns.append({
                    "name": header,
                    "type": col_type
                })

            # Create indexes dict from index_info and ensure primary key has B+ tree index
            indexes = {}
            if index_info:
                indexes[index_info["column"]] = index_info["type"]
            if primary_key:
                print(f"[DEBUG] Adding B+ tree index for primary key column: {primary_key}")
                indexes[primary_key] = 'bplus'

            # Create the table structure
            table_info = self.table_manager.create_table(
                table_name=table_name,
                columns=columns,
                indexes=indexes,
                primary_key=primary_key
            )

            # Check for table creation errors
            if table_info.get("status") == "error":
                return table_info

            # Import data from CSV
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    values = [row[header] for header in headers]
                    record = TypeConverter.convert_record(values, columns, table_info["format_str"])
                    result = self.table_manager.append_record(table_name, record)
                    if isinstance(result, dict) and result.get("status") == "error":
                        return result

            # Initialize indexes after data import
            index_result = self._initialize_indexes(table_name)
            if index_result.get("status") == "error":
                return index_result

            return {
                "status": "success",
                "message": f"Table {table_name} created successfully from file {file_path}"
            }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }

    def _initialize_indexes(self, table_name: str) -> Dict[str, Any]:
        """Initialize all indexes for a table"""
        print(f"[DEBUG] Starting _initialize_indexes for {table_name}")  # DEBUG
        table_info = self.table_manager.get_table_info(table_name)
        if not table_info:
            print(f"[DEBUG] Table {table_name} not found in _initialize_indexes")  # DEBUG
            return {"error": f"Table {table_name} not found"}

        try:
            for col, index_type in table_info["indexes"].items():
                print(f"[DEBUG] Initializing index for column {col}")  # DEBUG
                # Get column position
                col_info = next(
                    (c for c in table_info["columns"] if c["name"] == col),
                    None
                )
                if not col_info:
                    print(f"[DEBUG] Column {col} not found in table {table_name}")  # DEBUG
                    return {"error": f"Column {col} not found in table {table_name}"}
                
                col_idx = next(
                    (i for i, c in enumerate(table_info["columns"]) if c["name"] == col),
                    -1
                )
                print(f"[DEBUG] Column {col} found at position {col_idx}")  # DEBUG
                
                try:
                    print(f"[DEBUG] Creating index object for {col}")  # DEBUG
                    index = IndexFactory.get_index(
                        index_type='bplus',  # Currently hardcoded as we only support B+ trees
                        index_filename=table_info["index_files"][col],
                        data_filename=table_info["data_file"],
                        data_format=table_info["format_str"],
                        key_position=col_idx
                    )
                    
                    print(f"[DEBUG] Building index from data for {col}")  # DEBUG
                    # Build index from existing data
                    index.build_from_data()
                    print(f"[DEBUG] Successfully built index for {col}")  # DEBUG
                    
                except Exception as e:
                    print(f"[DEBUG] Error creating index for {col}: {str(e)}")  # DEBUG
                    return {
                        "status": "error",
                        "message": f"Failed to create index {index_type} for column {col}: {str(e)}"
                    }
            
            print("[DEBUG] Successfully initialized all indexes")  # DEBUG
            return {
                "status": "success",
                "message": "Table created successfully with all specified indexes."
            }
            
        except Exception as e:
            print(f"[DEBUG] Error in _initialize_indexes: {str(e)}")  # DEBUG
            return {
                "status": "error", 
                "message": f"Failed to initialize indexes: {str(e)}"
            }

    def _infer_type(self, value: str) -> str:
        """Infer column type from value"""
        try:
            int(value)
            return 'INT'
        except ValueError:
            try:
                float(value)
                return 'FLOAT'
            except ValueError:
                if ',' in value and value.count(',') == 1:
                    return 'ARRAY[FLOAT]'
                elif self._is_date(value):
                    return 'DATE'
                else:
                    return 'VARCHAR[50]'

    def _is_date(self, value: str) -> bool:
        """Check if value matches date format YYYY-MM-DD"""
        try:
            from datetime import datetime
            datetime.strptime(value, '%Y-%m-%d')
            return True
        except ValueError:
            return False 