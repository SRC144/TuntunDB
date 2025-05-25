import os
import struct
from typing import Any, Optional, List
from ...cursors.line_cursor import LineCursor
from ...cursors import BlockCursor

class SequentialFileIndex:
    
    def __init__(self, data_filename: str, data_format: str, key_position: int = 0, block_size: int = 4096):

        try:
            self.record_size = struct.calcsize(data_format)
        except struct.error:
            raise ValueError(f"Invalid data format: {data_format}")
        
        self.data_filename = data_filename
        self.data_format = data_format
        self.key_position = key_position
        self.block_size = block_size
        
        # Initialize cursors
        self.table_info = {
            "data_file": data_filename,
            "format_str": data_format,
            "columns": []  # This would need to be populated based on your schema
        }
        
        # Overflow area management
        self.overflow_filename = f"{data_filename}.overflow"
        self._init_files()
        
    def _init_files(self):
        try:
            # Initialize main data file with BlockCursor
            with BlockCursor(self.data_filename, self.block_size) as cursor:
                if cursor.total_blocks() == 0:
                    cursor.append_block(b'\x00' * self.block_size)  # Initial block
                    
            # Initialize overflow file with LineCursor
            with LineCursor(self.table_info) as lc:
                if lc.total_lines() == 0:
                    pass  # Just ensure file exists
        except IOError as e:
            raise IOError(f"Failed to initialize files: {str(e)}")
    
    def _extract_key(self, record_data: bytes) -> Any:
        try:
            unpacked = struct.unpack(self.data_format, record_data)
            return unpacked[self.key_position]
        except struct.error:
            raise ValueError("Invalid record data format")
    
    def _read_record(self, position: int, from_overflow: bool = False) -> Optional[bytes]:
        try:
            if from_overflow:
                with LineCursor(self.table_info) as lc:
                    lc.goto_line(position)
                    return lc.file.read(self.record_size)
            else:
                with BlockCursor(self.data_filename, self.block_size) as cursor:
                    block_num = position * self.record_size // self.block_size
                    offset = (position * self.record_size) % self.block_size
                    cursor.goto_block(block_num)
                    block_data = cursor.read()
                    if block_data and len(block_data) > offset:
                        return block_data[offset:offset+self.record_size]
                    return None
        except IOError as e:
            raise IOError(f"Failed to read record: {str(e)}")
    
    def _write_record(self, position: int, record_data: bytes, to_overflow: bool = False):
        if len(record_data) != self.record_size:
            raise ValueError(f"Record size must be {self.record_size} bytes")
        
        try:
            if to_overflow:
                with LineCursor(self.table_info) as lc:
                    lc.goto_line(position)
                    lc.file.write(record_data)
            else:
                with BlockCursor(self.data_filename, self.block_size) as cursor:
                    block_num = position * self.record_size // self.block_size
                    offset = (position * self.record_size) % self.block_size
                    
                    # Read the entire block
                    cursor.goto_block(block_num)
                    block_data = cursor.read() or bytearray(self.block_size)
                    
                    # Modify the specific record within the block
                    block_data = bytearray(block_data)
                    block_data[offset:offset+self.record_size] = record_data
                    
                    # Write back the modified block
                    cursor.goto_block(block_num)
                    cursor.overwrite_current(bytes(block_data))
        except IOError as e:
            raise IOError(f"Failed to write record: {str(e)}")
    
    def _append_record(self, record_data: bytes, to_overflow: bool = False) -> int:
        if len(record_data) != self.record_size:
            raise ValueError(f"Record size must be {self.record_size} bytes")
        
        try:
            if to_overflow:
                with LineCursor(self.table_info) as lc:
                    pos = lc.total_lines()
                    lc.append_record([0] * len(struct.unpack(self.data_format, record_data)))  # Dummy append
                    lc.goto_line(pos)
                    lc.file.write(record_data)
                    return pos
            else:
                with BlockCursor(self.data_filename, self.block_size) as cursor:
                    file_size = cursor.total_blocks() * self.block_size
                    pos = file_size // self.record_size
                    
                    # Calculate position in blocks
                    block_num = pos * self.record_size // self.block_size
                    offset = (pos * self.record_size) % self.block_size
                    
                    # If new record fits in existing block
                    if offset + self.record_size <= self.block_size:
                        cursor.goto_block(block_num)
                        block_data = cursor.read() or bytearray(self.block_size)
                        block_data = bytearray(block_data)
                        block_data[offset:offset+self.record_size] = record_data
                        cursor.overwrite_current(bytes(block_data))
                    else:
                        # Need to add a new block
                        new_block = bytearray(self.block_size)
                        new_block[0:self.record_size] = record_data
                        cursor.append_block(bytes(new_block))
                    return pos
        except IOError as e:
            raise IOError(f"Failed to append record: {str(e)}")
    
    def _find_insert_position(self, key: Any) -> int:
        with BlockCursor(self.data_filename, self.block_size) as cursor:
            file_size = cursor.total_blocks() * self.block_size
            if file_size == 0:
                return 0  # Empty file, insert at position 0
                
            low = 0
            high = (file_size // self.record_size) - 1
            
            while low <= high:
                mid = (low + high) // 2
                mid_record = self._read_record(mid)
                if not mid_record:
                    break
                    
                mid_key = self._extract_key(mid_record)
                
                if mid_key < key:
                    low = mid + 1
                elif mid_key > key:
                    high = mid - 1
                else:
                    return mid  # Key already exists
                    
            return low
    
    def _merge_files(self):
        try:
            # Read all records from both files
            all_records = []
            
            # Read main data file using BlockCursor
            with BlockCursor(self.data_filename, self.block_size) as cursor:
                file_size = cursor.total_blocks() * self.block_size
                num_records = file_size // self.record_size
                
                for i in range(num_records):
                    record = self._read_record(i)
                    if record:
                        all_records.append(record)
            
            # Read overflow area using LineCursor
            with LineCursor(self.table_info) as lc:
                num_records = lc.total_lines()
                for i in range(num_records):
                    record = self._read_record(i, from_overflow=True)
                    if record:
                        all_records.append(record)
            
            # Sort all records by key
            all_records.sort(key=self._extract_key)
            
            # Write back to main data file and clear overflow
            with BlockCursor(self.data_filename, self.block_size) as cursor:
                # Clear existing data
                cursor.goto_block(0)
                for _ in range(cursor.total_blocks()):
                    cursor.overwrite_current(b'\x00' * self.block_size)
                
                # Write sorted records
                for record in all_records:
                    self._append_record(record)
            
            # Clear overflow area
            with LineCursor(self.table_info) as lc:
                for i in range(lc.total_lines()):
                    lc.goto_line(i)
                    lc.file.write(b'\x00' * self.record_size)
        except IOError as e:
            raise IOError(f"Failed to merge files: {str(e)}")
    
    def build_from_data(self):
        with BlockCursor(self.data_filename, self.block_size) as cursor:
            if cursor.total_blocks() == 0:
                return
                
        try:
            # Read all records
            records = []
            with BlockCursor(self.data_filename, self.block_size) as cursor:
                file_size = cursor.total_blocks() * self.block_size
                num_records = file_size // self.record_size
                
                for i in range(num_records):
                    record = self._read_record(i)
                    if record:
                        records.append(record)
            
            # Sort by key
            records.sort(key=self._extract_key)
            
            # Write back sorted
            with BlockCursor(self.data_filename, self.block_size) as cursor:
                # Clear existing data
                cursor.goto_block(0)
                for _ in range(cursor.total_blocks()):
                    cursor.overwrite_current(b'\x00' * self.block_size)
                
                # Write sorted records
                for record in records:
                    self._append_record(record)
        except IOError as e:
            raise IOError(f"Failed to build index: {str(e)}")
    
    def add(self, record_data: bytes):
        if len(record_data) != self.record_size:
            raise ValueError(f"Record size must be {self.record_size} bytes")
        
        key = self._extract_key(record_data)
        
        # First check if key exists in main file
        pos = self._find_insert_position(key)
        existing_record = self._read_record(pos)
        if existing_record and self._extract_key(existing_record) == key:
            raise ValueError(f"Key {key} already exists")
        
        # Add to overflow area
        self._append_record(record_data, to_overflow=True)
        
        # Merge if overflow area gets too large
        with LineCursor(self.table_info) as lc:
            if lc.total_lines() > (self.block_size // self.record_size):
                self._merge_files()
    
    def search(self, key: Any) -> Optional[bytes]:
        # Search in main data file
        with BlockCursor(self.data_filename, self.block_size) as cursor:
            file_size = cursor.total_blocks() * self.block_size
            if file_size > 0:
                low = 0
                high = (file_size // self.record_size) - 1
                
                while low <= high:
                    mid = (low + high) // 2
                    mid_record = self._read_record(mid)
                    if not mid_record:
                        break
                        
                    mid_key = self._extract_key(mid_record)
                    
                    if mid_key == key:
                        return mid_record
                    elif mid_key < key:
                        low = mid + 1
                    else:
                        high = mid - 1
        
        # If not found in main file, check overflow area
        with LineCursor(self.table_info) as lc:
            for i in range(lc.total_lines()):
                record = self._read_record(i, from_overflow=True)
                if record and self._extract_key(record) == key:
                    return record
        
        return None
    
    def range_search(self, begin: Any, end: Any) -> List[bytes]:

        results = []
        
        # Search in main data file
        with BlockCursor(self.data_filename, self.block_size) as cursor:
            file_size = cursor.total_blocks() * self.block_size
            if file_size > 0:
                # Find starting position
                low = 0
                high = (file_size // self.record_size) - 1
                start_pos = 0
                
                while low <= high:
                    mid = (low + high) // 2
                    mid_record = self._read_record(mid)
                    if not mid_record:
                        break
                        
                    mid_key = self._extract_key(mid_record)
                    
                    if mid_key < begin:
                        low = mid + 1
                    else:
                        high = mid - 1
                
                start_pos = low
                
                # Read sequentially from start_pos
                for i in range(start_pos, (file_size // self.record_size)):
                    record = self._read_record(i)
                    if not record:
                        break
                        
                    record_key = self._extract_key(record)
                    if record_key > end:
                        break
                    if begin <= record_key <= end:
                        results.append(record)
        
        # Search in overflow area
        with LineCursor(self.table_info) as lc:
            for i in range(lc.total_lines()):
                record = self._read_record(i, from_overflow=True)
                if record:
                    record_key = self._extract_key(record)
                    if begin <= record_key <= end:
                        results.append(record)
        
        # Sort all results by key (since overflow may be out of order)
        results.sort(key=self._extract_key)
        return results
    
    def remove(self, key: Any) -> bool:
        # First try to find in main file
        found = False
        temp_filename = f"{self.data_filename}.temp"
        
        try:
            # Process main data file
            with BlockCursor(self.data_filename, self.block_size) as cursor, \
                 BlockCursor(temp_filename, self.block_size) as temp_cursor:
                
                file_size = cursor.total_blocks() * self.block_size
                num_records = file_size // self.record_size
                
                for i in range(num_records):
                    record = self._read_record(i)
                    if record and self._extract_key(record) == key:
                        found = True
                    else:
                        if record:
                            self._append_record(record, to_overflow=False)
            
            # If found in main file, replace file
            if found:
                os.replace(temp_filename, self.data_filename)
                return True
            
            # If not found in main file, check overflow
            temp_line_filename = f"{self.overflow_filename}.temp"
            found_in_overflow = False
            with LineCursor(self.table_info) as lc, \
                 open(temp_line_filename, 'wb') as temp_file:
                
                for i in range(lc.total_lines()):
                    record = self._read_record(i, from_overflow=True)
                    if record and self._extract_key(record) == key:
                        found_in_overflow = True
                    else:
                        if record:
                            temp_file.write(record)
            
            if found_in_overflow:
                os.replace(temp_line_filename, self.overflow_filename)
                return True
            
            return False
        except IOError as e:
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            if os.path.exists(temp_line_filename):
                os.remove(temp_line_filename)
            raise IOError(f"Failed to remove record: {str(e)}")
        finally:
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            if os.path.exists(temp_line_filename):
                os.remove(temp_line_filename)
    
    def get_all_records(self) -> List[bytes]:
        self._merge_files()  # Ensure all records are in main file and sorted
        records = []
        
        with BlockCursor(self.data_filename, self.block_size) as cursor:
            file_size = cursor.total_blocks() * self.block_size
            num_records = file_size // self.record_size
            
            for i in range(num_records):
                record = self._read_record(i)
                if record:
                    records.append(record)
        
        return records
