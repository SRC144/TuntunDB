import os
import struct
import math
from typing import Any, Optional, List
from ...cursors.line_cursor import LineCursor

class SequentialFileIndex:
    def __init__(self, index_filename: str, data_filename: str, data_format: str, key_position: int = 0):
        self.index_filename = index_filename
        self.data_filename = data_filename
        self.data_format = data_format
        self.key_position = key_position
        self.record_size = struct.calcsize(data_format)
        
        # Auxiliary file for unsorted records
        self.aux_filename = f"{index_filename}.aux"
        
        # Initialize files if they don't exist
        if not os.path.exists(self.index_filename):
            with open(self.index_filename, 'wb') as f:
                pass
                
        if not os.path.exists(self.aux_filename):
            with open(self.aux_filename, 'wb') as f:
                pass
    
    def _extract_key(self, data: bytes) -> int:
        """Extract key from record data as uint64"""
        try:
            unpacked = struct.unpack(self.data_format, data)
            return unpacked[self.key_position]
        except struct.error:
            raise ValueError("Invalid record data format")
    
    def _read_record(self, file_path: str, position: int) -> Optional[bytes]:
        """Read a record from a file at given position"""
        try:
            with open(file_path, 'rb') as f:
                f.seek(position * self.record_size)
                data = f.read(self.record_size)
                if len(data) == self.record_size:
                    return data
                return None
        except IOError:
            return None
    
    def _write_record(self, file_path: str, position: int, record_data: bytes):
        """Write a record to a file at given position"""
        with open(file_path, 'r+b') as f:
            f.seek(position * self.record_size)
            f.write(record_data)
    
    def _append_record(self, file_path: str, record_data: bytes) -> int:
        """Append a record to a file and return its position"""
        file_size = os.path.getsize(file_path)
        position = file_size // self.record_size
        with open(file_path, 'ab') as f:
            f.write(record_data)
        return position
    
    def _get_aux_file_size(self) -> int:
        """Get number of records in auxiliary file"""
        return os.path.getsize(self.aux_filename) // self.record_size
    
    def _get_main_file_size(self) -> int:
        """Get number of records in main index file"""
        return os.path.getsize(self.index_filename) // self.record_size
    
    def _should_rebuild(self) -> bool:
        """Check if auxiliary file size exceeds log n"""
        main_size = self._get_main_file_size()
        aux_size = self._get_aux_file_size()
        if main_size == 0:
            return aux_size > 1
        return aux_size > math.log2(main_size)
    
    def _merge_files(self):
        """Merge auxiliary and main files, removing marked records"""
        # Read all valid records
        records = []
        
        # Read from main file
        main_size = self._get_main_file_size()
        for i in range(main_size):
            record = self._read_record(self.index_filename, i)
            if record and not record.startswith(b'\x00'):  # Not marked as deleted
                records.append(record)
        
        # Read from auxiliary file
        aux_size = self._get_aux_file_size()
        for i in range(aux_size):
            record = self._read_record(self.aux_filename, i)
            if record and not record.startswith(b'\x00'):  # Not marked as deleted
                records.append(record)
        
        # Sort records by key
        records.sort(key=self._extract_key)
        
        # Write back to main file
        with open(self.index_filename, 'wb') as f:
            for record in records:
                f.write(record)
        
        # Clear auxiliary file
        with open(self.aux_filename, 'wb') as f:
            pass
    
    def build_from_data(self):
        """Build index from data file"""
        # Clear existing files
        with open(self.index_filename, 'wb') as f:
            pass
        with open(self.aux_filename, 'wb') as f:
            pass
        
        # Read all records from data file and sort them
        records = []
        with open(self.data_filename, 'rb') as f:
            while True:
                record = f.read(self.record_size)
                if not record or len(record) < self.record_size:
                    break
                records.append(record)
        
        # Sort records by key
        records.sort(key=self._extract_key)
        
        # Write sorted records to main index file
        with open(self.index_filename, 'wb') as f:
            for record in records:
                f.write(record)
    
    def add(self, record_data: bytes):
        """Add a record to the index"""
        if len(record_data) != self.record_size:
            raise ValueError(f"Record size must be {self.record_size} bytes")
        
        # Add to auxiliary file
        self._append_record(self.aux_filename, record_data)
        
        # Check if we need to rebuild
        if self._should_rebuild():
            self._merge_files()
    
    def search(self, key: int) -> Optional[bytes]:
        """Search for a record by key (expects uint64)"""
        # Binary search in main file
        left = 0
        right = self._get_main_file_size() - 1
        
        while left <= right:
            mid = (left + right) // 2
            record = self._read_record(self.index_filename, mid)
            if not record or record.startswith(b'\x00'):  # Skip deleted records
                right = mid - 1
                continue
                
            mid_key = self._extract_key(record)
            if mid_key == key:
                return record
            elif mid_key < key:
                left = mid + 1
            else:
                right = mid - 1
        
        # Linear search in auxiliary file
        aux_size = self._get_aux_file_size()
        for i in range(aux_size):
            record = self._read_record(self.aux_filename, i)
            if record and not record.startswith(b'\x00'):  # Not marked as deleted
                if self._extract_key(record) == key:
                    return record
        
        return None
    
    def range_search(self, begin: int, end: int) -> List[bytes]:
        """Search for records in a range (expects uint64)"""
        results = []
        
        # Binary search for start position in main file
        left = 0
        right = self._get_main_file_size() - 1
        start_pos = 0
        
        while left <= right:
            mid = (left + right) // 2
            record = self._read_record(self.index_filename, mid)
            if not record or record.startswith(b'\x00'):
                right = mid - 1
                continue
                
            mid_key = self._extract_key(record)
            if mid_key < begin:
                left = mid + 1
            else:
                right = mid - 1
                start_pos = mid
        
        # Sequential scan from start position in main file
        main_size = self._get_main_file_size()
        for i in range(start_pos, main_size):
            record = self._read_record(self.index_filename, i)
            if not record or record.startswith(b'\x00'):
                continue
                
            key = self._extract_key(record)
            if key > end:
                break
            if begin <= key <= end:
                results.append(record)
        
        # Check auxiliary file
        aux_size = self._get_aux_file_size()
        for i in range(aux_size):
            record = self._read_record(self.aux_filename, i)
            if record and not record.startswith(b'\x00'):
                key = self._extract_key(record)
                if begin <= key <= end:
                    results.append(record)
        
        # Sort results by key
        results.sort(key=self._extract_key)
        return results
    
    def remove(self, key: Any) -> bool:
        """Remove a record by key (mark as deleted)"""
        # Search in main file
        left = 0
        right = self._get_main_file_size() - 1
        
        while left <= right:
            mid = (left + right) // 2
            record = self._read_record(self.index_filename, mid)
            if not record or record.startswith(b'\x00'):
                right = mid - 1
                continue
                
            mid_key = self._extract_key(record)
            if mid_key == key:
                # Mark record as deleted
                self._write_record(self.index_filename, mid, b'\x00' * self.record_size)
                return True
            elif mid_key < key:
                left = mid + 1
            else:
                right = mid - 1
        
        # Search in auxiliary file
        aux_size = self._get_aux_file_size()
        for i in range(aux_size):
            record = self._read_record(self.aux_filename, i)
            if record and not record.startswith(b'\x00'):
                if self._extract_key(record) == key:
                    # Mark record as deleted
                    self._write_record(self.aux_filename, i, b'\x00' * self.record_size)
                    return True
        
        return False