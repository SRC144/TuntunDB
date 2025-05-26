from typing import Dict, Any, List
import csv
import os
import struct
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
        table_name = parsed_query["table_name"]
        columns = parsed_query["columns"]
        indexes = parsed_query.get("indexes", {})
        primary_key = parsed_query.get("primary_key")

        # If there's a primary key, ensure it has a B+ tree index
        if primary_key:
            indexes[primary_key] = 'bplus'

        # Create the table structure first
        table_info = self.table_manager.create_table(
            table_name=table_name,
            columns=columns,
            indexes=indexes,
            primary_key=primary_key
        )

        # Check for table creation errors
        if table_info.get("status") == "error":
            return table_info

        # Initialize indexes
        index_result = self._initialize_indexes(table_name)
        if index_result.get("status") == "error":
            return index_result
        
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
            # First pass: analyze data to determine proper column sizes and types
            max_lengths = {}
            column_types = {}  # Track potential types for each column
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = [h.strip() for h in next(reader)]
                
                # Initialize tracking dictionaries
                for header in headers:
                    max_lengths[header] = 0
                    column_types[header] = {'has_float': False, 'all_int': True, 'is_array': False}
                
                # Analyze all rows
                for row in reader:
                    for header, value in zip(headers, row):
                        value = value.strip()
                        max_lengths[header] = max(max_lengths[header], len(value))
                        
                        # Clean value of thousand separators
                        clean_val = value.replace(',', '')
                        
                        # Try parsing as number if it looks numeric
                        if clean_val.replace('.', '').replace('-', '').isdigit():
                            try:
                                float_val = float(clean_val)
                                if float_val != int(float_val):
                                    column_types[header]['has_float'] = True
                                    column_types[header]['all_int'] = False
                            except ValueError:
                                column_types[header]['all_int'] = False
                        else:
                            # Check if it's an array of floats
                            if self._is_array_float(value):
                                column_types[header]['is_array'] = True
                            column_types[header]['all_int'] = False

            # Reopen file for actual processing
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = [h.strip() for h in next(reader)]
                remaining_rows = [[v.strip() for v in row] for row in reader]

            # Infer column types with proper VARCHAR sizes
            columns = []
            primary_key = None
            for header in headers:
                if column_types[header]['is_array']:
                    col_type = 'ARRAY[FLOAT]'
                elif column_types[header]['has_float']:
                    col_type = 'FLOAT'
                elif column_types[header]['all_int']:
                    col_type = 'INT'
                else:
                    # Add 20% padding for safety and round up to nearest 10
                    size = max_lengths[header]
                    padded_size = min(255, int(size * 1.2 + 10))
                    col_type = f'VARCHAR[{padded_size}]'
                
                if not primary_key:
                    primary_key = header
                columns.append({
                    "name": header,
                    "type": col_type
                })

            # Create indexes dict from index_info and ensure primary key has B+ tree index
            indexes = {}
            if index_info:
                indexes.update(index_info)
            if primary_key:
                print(f"[DEBUG] Adding B+ tree index for primary key column: {primary_key}")
                indexes[primary_key] = 'bplus'

            # Create the table structure WITH indexes first
            table_info = self.table_manager.create_table(
                table_name=table_name,
                columns=columns,
                indexes=indexes,
                primary_key=primary_key
            )

            # Check for table creation errors
            if table_info.get("status") == "error":
                return table_info

            # Initialize indexes before adding data
            index_result = self._initialize_indexes(table_name)
            if index_result.get("status") == "error":
                return index_result

            # Now process all rows
            for row_num, row in enumerate(remaining_rows, start=1):
                # Clean and validate each value based on column type
                cleaned_values = []
                
                for col_idx, (value, col) in enumerate(zip(row, columns)):
                    value = value.strip()
                    if col["type"] == "INT":
                        try:
                            val = int(value.replace(',', ''))
                            cleaned_values.append(val)
                        except ValueError:
                            cleaned_values.append(0)
                    elif col["type"].startswith("VARCHAR"):
                        # Format string with proper position encoding
                        pos_bytes = row_num.to_bytes(4, byteorder='little', signed=False)
                        pos_str = ''.join(chr(b) for b in pos_bytes)
                        # Add proper null byte padding
                        prefix = chr(0) * 4
                        cleaned_values.append(prefix + pos_str + value)
                    elif col["type"] == "FLOAT":
                        try:
                            cleaned_values.append(float(value.replace(',', '')))
                        except ValueError:
                            cleaned_values.append(0.0)
                    elif col["type"] == "ARRAY[FLOAT]":
                        if ',' in value:
                            parts = value.split(',')
                            try:
                                # Handle each part separately to account for thousand separators
                                val1 = float(parts[0].strip().replace(',', ''))
                                val2 = float(parts[1].strip().replace(',', '')) if len(parts) > 1 else 0.0
                                cleaned_values.extend([val1, val2])
                            except ValueError:
                                cleaned_values.extend([0.0, 0.0])
                        else:
                            try:
                                val = float(value.replace(',', ''))
                                cleaned_values.extend([val, 0.0])
                            except ValueError:
                                cleaned_values.extend([0.0, 0.0])

                data_bytes = struct.pack(
                    table_info["record_format"],   # e.g. "=i50s52s..."
                    *[ TypeConverter.convert_value(v, col["type"])
                    for v, col in zip(cleaned_values, table_info["columns"]) ]
                )

                result = self.table_manager.append_record(table_name, data_bytes)
                if isinstance(result, dict) and result.get("status") == "error":
                    return result

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
        table_info = self.table_manager.get_table_info(table_name)
        if not table_info:
            return {"error": f"Table {table_name} not found"}

        try:
            # First validate all columns exist before starting any index creation
            for col in table_info["indexes"].keys():
                col_info = next(
                    (c for c in table_info["columns"] if c["name"] == col),
                    None
                )
                if not col_info:
                    return {"error": f"Column {col} not found in table {table_name}"}

            # Now create and build each index
            for col, index_type in table_info["indexes"].items():
                
                col_idx = next(
                    (i for i, c in enumerate(table_info["columns"]) if c["name"] == col),
                    -1
                )
                
                try:
                    index = None
                    try:
                        index = IndexFactory.get_index(
                            index_type='bplus',  # Currently hardcoded as we only support B+ trees
                            index_filename=table_info["index_files"][col],
                            data_filename=table_info["data_file"],
                            data_format=table_info["format_str"],
                            key_position=col_idx + 1  # +1 to account for deletion marker
                        )
                        
                        # Build index from existing data
                        index.build_from_data()
                    finally:
                        # Ensure index is properly closed/cleaned up
                        if index and hasattr(index, 'close'):
                            index.close()
                    
                except Exception as e:
                    return {
                        "status": "error",
                        "message": f"Failed to create index {index_type} for column {col}: {str(e)}"
                    }
            
            return {
                "status": "success",
                "message": "Table created successfully with all specified indexes."
            }
            
        except Exception as e:
            return {
                "status": "error", 
                "message": f"Failed to initialize indexes: {str(e)}"
            }

    def _infer_type(self, value: str) -> str:
        """Infer column type from value"""
        # First clean the value by removing thousand separators
        cleaned_value = value.replace(',', '')
        
        try:
            int(cleaned_value)
            return 'INT'
        except ValueError:
            try:
                float(cleaned_value)
                return 'FLOAT'
            except ValueError:
                if self._is_array_float(value):
                    return 'ARRAY[FLOAT]'
                elif self._is_date(value):
                    return 'DATE'
                else:
                    return 'VARCHAR[50]'

    def _is_array_float(self, value: str) -> bool:
        """Check if value is an array of floats (x,y format)"""
        if ',' not in value:
            return False
            
        # If it has a comma, check if it's just a thousands separator
        # by looking at the pattern: digits,digits where second part is exactly 3 digits
        parts = value.split(',')
        if len(parts) == 2 and len(parts[1].strip()) == 3 and parts[1].strip().isdigit():
            return False  # This is likely a number with thousand separator
            
        # Try to parse both parts as floats
        try:
            for part in parts:
                # Clean each part of thousand separators
                clean_part = part.strip().replace(',', '')
                float(clean_part)
            return True
        except ValueError:
            return False

    def _is_date(self, value: str) -> bool:
        """Check if value matches date format YYYY-MM-DD"""
        try:
            from datetime import datetime
            datetime.strptime(value, '%Y-%m-%d')
            return True
        except ValueError:
            return False 