import os
import io

class BlockCursor:
    def __init__(self, filename, block_size):
        self.filename = filename
        self.block_size = block_size
        self.position = 0  # num de bloque actual (0-based)

        if os.path.exists(filename):
            self.file = open(filename, 'r+b')
        else:
            self.file = open(filename, 'w+b')

    def read(self):
        """Lee y retorna el bloque en bytes."""
        self.file.seek(self.position * self.block_size)
        data = self.file.read(self.block_size)
        if len(data) < self.block_size:
            return None
        return data

    def advance_block(self):
        """Ir al siguiente bloque."""
        self.position += 1

    def goto_block(self, block_number):
        """Ir a bloque especifico"""
        self.position = block_number

    def append_block(self, data):
        """Agregar bloque al final."""
        if len(data) != self.block_size:
            raise ValueError(f"Data size must match block size ({self.block_size} bytes)")

        self.file.seek(0, io.SEEK_END)
        self.file.write(data)
        self.position = self.file.tell() // self.block_size  # Update position

    def overwrite_current(self, data):
        """Sobreescribir bloque."""
        if len(data) != self.block_size:
            raise ValueError(f"Data size must match block size ({self.block_size} bytes)")

        self.file.seek(self.position * self.block_size)
        self.file.write(data)

    def eof(self):
        """Verificar si el cursor esta al final del archivo."""
        return self.position >= self.total_blocks()

    def _file_size(self):
        """obtener tamanho del archivo."""
        current_pos = self.file.tell()
        self.file.seek(0, io.SEEK_END)
        size = self.file.tell()
        self.file.seek(current_pos)
        return size

    def current_block_number(self):
        """Return bloque actual."""
        return self.position

    def goto_start(self):
        self.position = 0

    def goto_end(self):
        self.position = self.total_blocks()

    def close(self):
        self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def total_blocks(self):
        return self._file_size() // self.block_size

    def read_block(self, block_number):
        """Leer bloque sin cambiar posicion del cursor."""
        current_pos = self.position
        self.goto_block(block_number)
        data = self.read()
        self.goto_block(current_pos)
        return data

    def update_block(self, block_number, data):
        """Actualizar bloque."""
        if len(data) != self.block_size:
            raise ValueError(f"Data size must match block size ({self.block_size} bytes)")

        current_pos = self.position
        self.goto_block(block_number)
        self.overwrite_current(data)
        self.goto_block(current_pos)

    def flush(self):
        """Ensure writes are flushed to disk."""
        self.file.flush()
