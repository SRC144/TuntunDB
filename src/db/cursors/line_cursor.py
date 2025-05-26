import os
import io

class LineCursor:
    """A cursor for reading fixed-length records from a binary file"""
    
    def __init__(self, filename: str, record_size: int):
        self.filename = filename
        self.record_size = record_size
        self.file = None
        print(f"[DEBUG] LineCursor: Initialized with file {filename} and record size {record_size}")
        self.position = 0  # current record number (0-based)
        
        if os.path.exists(filename):
            self.file = open(filename, 'r+b')
        else:
            self.file = open(filename, 'w+b')
        
    def read_record(self) -> bytes:
        """Read and return the current record in bytes."""
        if not self.file:
            raise ValueError("File not open")
            
        # Calculate the correct position in bytes
        byte_position = self.position * self.record_size
        print(f"[DEBUG] LineCursor: Reading from byte position {byte_position}")
        
        # Seek to the correct position
        self.file.seek(byte_position)
        
        # Read the record
        data = self.file.read(self.record_size)
        print(f"[DEBUG] LineCursor: Read record of size {len(data)}")
        
        if len(data) < self.record_size:
            return None
            
        return data

    def advance_record(self):
        """Move to next record."""
        self.position += 1

    def goto_record(self, record_number: int):
        """Go to specific record."""
        if not self.file:
            raise ValueError("File not open")
            
        # Update the current position
        self.position = record_number
        byte_position = self.position * self.record_size
        print(f"[DEBUG] LineCursor: Moving to record {record_number} at byte position {byte_position}")
        
        # Seek to the position
        self.file.seek(byte_position)

    def append_record(self, data: bytes):
        """Append record to end of file."""
        if len(data) != self.record_size:
            raise ValueError(f"Data size must match record size ({self.record_size} bytes)")

        self.file.seek(0, io.SEEK_END)
        self.file.write(data)
        self.position = self.file.tell() // self.record_size

    def overwrite_current(self, data: bytes):
        """Overwrite current record."""
        if len(data) != self.record_size:
            raise ValueError(f"Data size must match record size ({self.record_size} bytes)")

        self.file.seek(self.position * self.record_size)
        self.file.write(data)

    def eof(self) -> bool:
        """Check if cursor is at end of file."""
        return self.position >= self.total_records()

    def _file_size(self) -> int:
        """Get file size in bytes."""
        current_pos = self.file.tell()
        self.file.seek(0, io.SEEK_END)
        size = self.file.tell()
        self.file.seek(current_pos)
        return size

    def current_record_number(self) -> int:
        """Return current record number."""
        return self.position

    def goto_start(self):
        """Go to first record."""
        self.position = 0

    def goto_end(self):
        """Go to last record."""
        self.position = self.total_records()

    def close(self):
        """Close the file."""
        print(f"[DEBUG] LineCursor: Closing file {self.filename}")
        if self.file:
            self.file.close()
            self.file = None

    def __enter__(self):
        """Open file for reading and writing"""
        print(f"[DEBUG] LineCursor: Opening file {self.filename}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close file"""
        self.close()

    def total_records(self) -> int:
        """Get total number of records."""
        if not self.file:
            raise ValueError("File not open")
        current = self.file.tell()
        self.file.seek(0, 2)  # Seek to end
        size = self.file.tell()
        self.file.seek(current)  # Restore position
        total = size // self.record_size
        print(f"[DEBUG] LineCursor: Total records: {total}")
        return total

    def read_at(self, record_number: int) -> bytes:
        """Read record at position without changing cursor position."""
        current_pos = self.position
        self.goto_record(record_number)
        data = self.read_record()
        self.goto_record(current_pos)
        return data

    def update_record(self, record_number: int, data: bytes):
        """Update record at position."""
        if len(data) != self.record_size:
            raise ValueError(f"Data size must match record size ({self.record_size} bytes)")

        current_pos = self.position
        self.goto_record(record_number)
        self.overwrite_current(data)
        self.goto_record(current_pos)

    def flush(self):
        """Ensure writes are flushed to disk."""
        self.file.flush() 