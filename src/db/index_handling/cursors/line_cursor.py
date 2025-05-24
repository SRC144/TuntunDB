import struct
import os
import io

class LineCursor:
    def __init__(self, filename, struct_format):
        self.filename = filename
        self.struct_format = struct_format
        self.struct_size = struct.calcsize(struct_format)
        self.position = 0  # Current byte position

        # read/write en modo binario
        if os.path.exists(filename):
            self.file = open(filename, 'r+b')
        else:
            self.file = open(filename, 'w+b')

    def read(self):
        """Lee y retorna la linea actual"""
        self.file.seek(self.position)
        data = self.file.read(self.struct_size)
        if len(data) < self.struct_size:
            return None
        return struct.unpack(self.struct_format, data)

    def advance_line(self):
        """ir a la siguiente linea"""
        self.position += self.struct_size

    def goto_line(self, line_number):#TOMA SOLO NUMERO DE LINEA
        """ir a linea especifica"""
        self.position = line_number * self.struct_size

    def append_line(self, data):
        """Agregar linea al final del archivo"""
        self.file.seek(0, io.SEEK_END)
        packed = struct.pack(self.struct_format, *data)
        self.file.write(packed)
        self.position = self.file.tell()

    def overwrite_current(self, data):
        """sobrescribir linea actual"""
        self.file.seek(self.position)
        packed = struct.pack(self.struct_format, *data)
        self.file.write(packed)

    def eof(self):
        """verificar end of file"""
        return self.position >= self._file_size()

    def _file_size(self):
        """obtener tamanho del archivo en bytes"""
        current_pos = self.file.tell()
        self.file.seek(0, io.SEEK_END)
        size = self.file.tell()
        self.file.seek(current_pos)
        return size

    def current_line_number(self):
        """Obtener numero de linea actual (0-based)"""
        return self.position // self.struct_size

    def goto_start(self):
        self.position = 0

    def goto_end(self):
        self.position = self._file_size()

    def close(self):
        self.file.close()

    # Context manager support
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def total_lines(self):
        """obtener numero total de lineas"""
        return self._file_size() // self.struct_size

    def read_line(self, line_number):
        """leer linea especifica"""
        self.goto_line(line_number)
        return self.read()

    def update_line(self, line_number, data):
        """actualizar linea especifica"""
        self.goto_line(line_number)
        self.overwrite_current(data)
