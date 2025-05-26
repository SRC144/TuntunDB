from src.db.engine.query_parser import QueryParser
from src.db.commands.create import CreateCommand
from src.db.commands.drop import DropCommand
from src.db.storage_management.table_manager import TableManager
import os


# Ensure data directory exists
os.makedirs('data', exist_ok=True)

# Initialize components
parser = QueryParser()
table_manager = TableManager('data')
create_command = CreateCommand(table_manager)
drop_command = DropCommand(table_manager)

# First drop the existing table if it exists
drop_query = "drop table cancer"
drop_parsed = parser.parse(drop_query)
drop_result = drop_command.execute(drop_parsed)
print(f'Drop result: {drop_result}')

drop_query = "drop table cars"
drop_parsed = parser.parse(drop_query)
drop_result = drop_command.execute(drop_parsed)
print(f'Drop result: {drop_result}')

# Parse and execute create table command
query = "create table cancer from file 'cancer_data.csv'"

parsed = parser.parse(query)
print(f'Parsed query: {parsed}')
result = create_command.execute(parsed)
print(f'Result: {result}') 

query = "create table cars from file 'car_prices_jordan.csv'"

parsed = parser.parse(query)
print(f'Parsed query: {parsed}')
result = create_command.execute(parsed)
print(f'Result: {result}') 