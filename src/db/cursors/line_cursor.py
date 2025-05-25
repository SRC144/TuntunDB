import struct
from typing import Dict, Any, Optional, List, Tuple
from ..storage_management.type_conversion import TypeConverter
import os

class LineCursor:
    """A cursor for reading fixed-length records from a binary file"""
    
    def __init__(self, table_info: Dict[str, Any]):
        self.table_info = table_info
        self.data_file = table_info["data_file"]
        self.format_str = table_info["format_str"]
        self.record_size = struct.calcsize(self.format_str)
        self.columns = table_info["columns"]
        self.file = None
        
    def __enter__(self):
        self.file = open(self.data_file, 'rb+' if os.path.exists(self.data_file) else 'wb+')
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()
            self.file = None
            
    def goto_line(self, line_number: int) -> None:
        """Move cursor to a specific line"""
        if not self.file:
            self.file = open(self.data_file, 'rb+' if os.path.exists(self.data_file) else 'wb+')
        self.file.seek(line_number * self.record_size)
        
    def total_lines(self) -> int:
        """Get total number of lines in file"""
        if not self.file:
            self.file = open(self.data_file, 'rb+' if os.path.exists(self.data_file) else 'wb+')
        current = self.file.tell()
        self.file.seek(0, 2)  # Seek to end
        total = self.file.tell() // self.record_size
        self.file.seek(current)  # Restore position
        return total
    
    def read_record(self, position: int) -> Optional[List[Any]]:
        """Read a single record at the given position"""
        if not self.file:
            self.file = open(self.data_file, 'rb+' if os.path.exists(self.data_file) else 'wb+')
        self.file.seek(position * self.record_size)
        try:
            data = self.file.read(self.record_size)
            if not data:
                return None
            return list(struct.unpack(self.format_str, data))
        except struct.error:
            return None
    
    def read_records(self, positions: List[int]) -> List[List[Any]]:
        """Read multiple records at the given positions"""
        if not self.file:
            self.file = open(self.data_file, 'rb+' if os.path.exists(self.data_file) else 'wb+')
        records = []
        for pos in positions:
            self.file.seek(pos * self.record_size)
            try:
                data = self.file.read(self.record_size)
                if data:
                    records.append(list(struct.unpack(self.format_str, data)))
            except struct.error:
                continue
        return records
    
    def append_record(self, values: List[Any]) -> int:
        """Append a new record and return its position"""
        if not self.file:
            self.file = open(self.data_file, 'rb+' if os.path.exists(self.data_file) else 'wb+')
            
        # Convert values to binary format
        binary_values = []
        for value, col in zip(values, self.columns):
            col_type = col["type"]
            if col_type.startswith("VARCHAR"):
                size = int(col_type.split('[')[1].split(']')[0])
                binary_value = TypeConverter.to_binary_value(value, col_type, size=size)
            else:
                binary_value = TypeConverter.to_binary_value(value, col_type)
                
            if isinstance(binary_value, tuple):
                binary_values.extend(binary_value)
            else:
                binary_values.append(binary_value)
                
        # Pack and write
        self.file.seek(0, 2)  # Seek to end
        position = self.file.tell() // self.record_size
        record = struct.pack(self.format_str, *binary_values)
        self.file.write(record)
        self.file.flush()  # Ensure data is written to disk
        return position
    
    def scan(self, conditions: Optional[List[Dict[str, Any]]] = None) -> List[List[Any]]:
        """Scan the table with optional filtering"""
        if not self.file:
            self.file = open(self.data_file, 'rb+' if os.path.exists(self.data_file) else 'wb+')
            
        records = []
        self.file.seek(0)
        while True:
            data = self.file.read(self.record_size)
            if not data:
                break
            try:
                record = list(struct.unpack(self.format_str, data))
                if self._matches_conditions(record, conditions):
                    records.append(record)
            except struct.error:
                break
        return records
    
    def _matches_conditions(self, record: List[Any], conditions: Optional[List[Dict[str, Any]]]) -> bool:
        """Check if a record matches all conditions"""
        if not conditions:
            return True
            
        for cond in conditions:
            col_idx = next(
                (i for i, c in enumerate(self.columns) if c["name"] == cond["column"]),
                -1
            )
            if col_idx == -1:
                continue
                
            col_type = self.columns[col_idx]["type"]
            record_value = record[col_idx]
            
            # Convert condition value to Python type for comparison
            cond_value = TypeConverter.to_python_value(cond["value"], col_type)
            
            if cond["operation"] == "=":
                if record_value != cond_value:
                    return False
            elif cond["operation"] == ">":
                if record_value <= cond_value:
                    return False
            elif cond["operation"] == "<":
                if record_value >= cond_value:
                    return False
            elif cond["operation"] == ">=":
                if record_value < cond_value:
                    return False
            elif cond["operation"] == "<=":
                if record_value > cond_value:
                    return False
                    
        return True 