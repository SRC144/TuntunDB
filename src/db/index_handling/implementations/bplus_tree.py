import struct
import os
import bisect
from ...cursors.line_cursor import LineCursor
from ...cursors import BlockCursor

# Constants for B+ tree
PAGE_SIZE = 4096  # 4KB pages
KEY_SIZE = 8  # 8 bytes for key (uint64)
PTR_SIZE = 8  # 8 bytes for pointer (signed long long)
PAGE_HEADER_SIZE = 16  # is_leaf(1) + num_keys(2) + page_id(4) + parent_id(8) + overflow(1)
HEADER_SIZE = 8  # root_block(8)

# Format strings
KEY_FORMAT = "=Q"  # 8 bytes for keys (unsigned long long)
PTR_FORMAT = "=q"  # 8 bytes for pointers (signed long long)
PAGE_HEADER_FORMAT = "=BHiq"  # is_leaf(1) + num_keys(2) + page_id(4) + parent_id(8)
HEADER_FORMAT = "=q"  # root_block(8)

class BPlusPage:
    """Base class for B+ tree pages"""
    def __init__(self, is_leaf, num_keys, page_id, parent_id):
        self.is_leaf = is_leaf
        self.num_keys = num_keys
        self.page_id = page_id
        self.parent_id = parent_id
        
    def header_bytes(self):
        """Pack page header into bytes"""
        return struct.pack(PAGE_HEADER_FORMAT,
            1 if self.is_leaf else 0,
            self.num_keys,
            self.page_id,
            self.parent_id
        )

    @staticmethod
    def pack_key(key: int) -> bytes:
        """Pack a uint64 key into bytes"""
        if not isinstance(key, int):
            raise ValueError("B+ tree keys must be integers")
        if key < 0 or key > 18446744073709551615:  # 2^64 - 1
            raise ValueError("B+ tree keys must be unsigned 64-bit integers")
        return struct.pack(KEY_FORMAT, key)
        
    @staticmethod
    def unpack_key(data: bytes, offset: int) -> tuple[int, int]:
        """Unpack a key and return (key, new_offset)"""
        key = struct.unpack(KEY_FORMAT, data[offset:offset + KEY_SIZE])[0]
        return key, offset + KEY_SIZE

class InternalPage(BPlusPage):
    def __init__(self, page_id, parent_id, keys, pointers):
        super().__init__(is_leaf=0, num_keys=len(keys), page_id=page_id, parent_id=parent_id)
        self.keys = keys
        self.pointers = pointers
        
    def pack(self):
        """Pack page into bytes"""
        data = self.header_bytes()
        
        # Pack keys and pointers
        for i, (key, ptr) in enumerate(zip(self.keys, self.pointers)):
            data += self.pack_key(key)
            data += struct.pack(PTR_FORMAT, ptr)
            
        # Pack last pointer
        data += struct.pack(PTR_FORMAT, self.pointers[-1])
        
        return data.ljust(PAGE_SIZE, b'\x00')

    @classmethod
    def unpack(cls, data, cursor=None):
        """Unpack page data"""
        header = data[:PAGE_HEADER_SIZE]
        is_leaf, num_keys, page_id, parent_id = struct.unpack(PAGE_HEADER_FORMAT, header)
        
        if is_leaf:
            raise ValueError("Not an internal page")
            
        # Read keys and pointers
        keys = []
        pointers = []
        offset = PAGE_HEADER_SIZE
        
        for i in range(num_keys):
            # Read key
            key, new_offset = cls.unpack_key(data, offset)
            # Read pointer
            ptr = struct.unpack(PTR_FORMAT, data[new_offset:new_offset + PTR_SIZE])[0]
            keys.append(key)
            pointers.append(ptr)
            offset = new_offset + PTR_SIZE
            
        # Read last pointer
        last_ptr = struct.unpack(PTR_FORMAT, data[offset:offset + PTR_SIZE])[0]
        pointers.append(last_ptr)
        
        return cls(page_id, parent_id, keys, pointers)

class LeafPage(BPlusPage):
    def __init__(self, page_id, parent_id, key_value_pairs, next_leaf):
        super().__init__(is_leaf=1, num_keys=len(key_value_pairs), page_id=page_id, parent_id=parent_id)
        self.key_value_pairs = key_value_pairs
        self.next_leaf = next_leaf
        
    def pack(self):
        """Pack page into bytes"""
        data = self.header_bytes()
        
        # Pack key-value pairs
        for key, ptr in self.key_value_pairs:
            data += self.pack_key(key)
            data += struct.pack(PTR_FORMAT, ptr)
            
        # Pack next leaf pointer
        data += struct.pack(PTR_FORMAT, self.next_leaf)
        
        return data.ljust(PAGE_SIZE, b'\x00')

    @classmethod
    def unpack(cls, data, cursor=None):
        """Unpack page data"""
        header = data[:PAGE_HEADER_SIZE]
        is_leaf, num_keys, page_id, parent_id = struct.unpack(PAGE_HEADER_FORMAT, header)
        
        if not is_leaf:
            raise ValueError("Not a leaf page")
            
        # Read key-value pairs
        key_value_pairs = []
        offset = PAGE_HEADER_SIZE
        
        for i in range(num_keys):
            # Read key
            key, new_offset = cls.unpack_key(data, offset)
            # Read pointer
            ptr = struct.unpack(PTR_FORMAT, data[new_offset:new_offset + PTR_SIZE])[0]
            key_value_pairs.append((key, ptr))
            offset = new_offset + PTR_SIZE
            
        # Read next leaf pointer
        next_leaf = struct.unpack(PTR_FORMAT, data[-PTR_SIZE:])[0]
        
        return cls(page_id, parent_id, key_value_pairs, next_leaf)

class BPlusTreeIndex:
    def __init__(self, index_filename, data_filename, data_format, key_position=0):
        self.index_filename = index_filename
        self.data_filename = data_filename
        self.data_format = data_format
        self.key_position = key_position
        self.root_block = 0
        self.record_size = struct.calcsize(data_format)

        # Calculate capacities based on fixed sizes
        self.internal_capacity = (PAGE_SIZE - PAGE_HEADER_SIZE - PTR_SIZE) // (KEY_SIZE + PTR_SIZE)
        self.leaf_capacity = (PAGE_SIZE - PAGE_HEADER_SIZE - PTR_SIZE) // (KEY_SIZE + PTR_SIZE)

        self._init_storage()

        #manejar mejor luego
        if data_filename:
            self.build_from_data()

    def _init_storage(self):
        file_exists = os.path.exists(self.index_filename)
        if not file_exists:
            self._create_empty_tree()
            return
            
        with BlockCursor(self.index_filename, PAGE_SIZE) as cursor:
            header_data = cursor.read_block(0)
            if header_data is None or len(header_data) == 0:
                # File exists but is empty, create new tree
                self._create_empty_tree()
                return
                
            # Ensure header is properly formatted
            try:
                self.root_block = struct.unpack(HEADER_FORMAT, header_data[:HEADER_SIZE])[0]
            except struct.error:
                raise ValueError("Invalid header block format")

    def _create_empty_tree(self):
        with BlockCursor(self.index_filename, PAGE_SIZE) as cursor:
            root_block = 1
            header_data = struct.pack(HEADER_FORMAT, root_block).ljust(HEADER_SIZE, b'\x00')
            cursor.append_block(header_data.ljust(PAGE_SIZE, b'\x00'))
            root_page = LeafPage(
                page_id=root_block,
                parent_id=-1,
                key_value_pairs=[],
                next_leaf=-1
            )
            cursor.append_block(root_page.pack())
        self.root_block = root_block

    def _parse_page(self, data):
        if not data:
            return None
        is_leaf = data[0]
        if is_leaf:
            return LeafPage.unpack(data)
        else:
            return InternalPage.unpack(data)

    def build_from_data(self):
        # Reiniciar índice en disco
        if os.path.exists(self.index_filename):
            os.remove(self.index_filename)
        self._init_storage()
        
        # Create table_info dict for LineCursor
        table_info = {
            "data_file": self.data_filename,
            "format_str": self.data_format
        }
        
        # Recorrer datos existentes y poblar árbol
        with LineCursor(table_info) as lc:
            total = lc.total_lines()
            for ptr in range(total):
                lc.goto_line(ptr)
                raw = lc.file.read(self.record_size)
                key = self._extract_key(raw)
                # Insertar directamente sin reescribir datos
                self._insert_entry(key, ptr)

    def search(self, key):
        with BlockCursor(self.index_filename, PAGE_SIZE) as cursor:
            current_block = self.root_block
            while True:
                data = cursor.read_block(current_block)
                if not data:
                    return None

                page = self._parse_page(data)
                if page is None:
                    return None

                if page.is_leaf:
                    assert isinstance(page, LeafPage)
                    # Búsqueda lineal en las hojas
                    for k, v in page.key_value_pairs:
                        if k == key:
                            return v
                    return None
                else:
                    assert isinstance(page, InternalPage)
                    idx = bisect.bisect_right(page.keys, key)
                    current_block = page.pointers[idx]

    def range_search(self, begin, end):
        ptrs = []
        with BlockCursor(self.index_filename, PAGE_SIZE) as cursor:
            # 1) Encontrar la hoja donde podría aparecer 'begin'
            leaf = self._find_leaf_page(cursor, begin, [])
            visited = set()
            # 2) Recorrer hojas sucesivas
            while leaf and leaf.page_id not in visited:
                visited.add(leaf.page_id)
                for k, ptr in leaf.key_value_pairs:
                    if k < begin:
                        continue
                    if k > end:
                        return ptrs
                    ptrs.append(ptr)
                # Avanzar a la siguiente hoja
                next_id = leaf.next_leaf
                if next_id is None or next_id < 0:
                    break
                data = cursor.read_block(next_id)
                leaf = self._parse_page(data)
            return ptrs

    def _find_child_page(self, page, key):
        """Determine which child page to follow using binary search"""
        idx = bisect.bisect_right(page.keys, key)
        return page.pointers[idx]

    def _find_leaf_page(self, cursor, key, stack):
        """Traverse from root to leaf, filling the parent stack"""
        current_block = self.root_block
        while True:
            data = cursor.read_block(current_block)
            page = self._parse_page(data)

            assert isinstance(page, BPlusPage) #para que no llore el linter

            if page.is_leaf:
                assert isinstance(page, LeafPage)
                return page

            stack.append(current_block)
            current_block = self._find_child_page(page, key)

    def _update_leaf(self, page, entries, cursor):
        """actualizar hoja sin split"""
        page.key_value_pairs = entries
        page.num_keys = len(entries)
        cursor.update_block(page.page_id, page.pack())

    def _insert_into_temp_entries(self, entries, key, ptr):
        """Inserta manteniendo el orden con búsqueda binaria directa"""
        new_entry = (key, ptr)

        # Casos extremos: lista vacía o clave mayor al último elemento
        if not entries:
            return [new_entry]
        if key > entries[-1][0]:
            return entries + [new_entry]

        # Búsqueda binaria manual en entries
        low, high = 0, len(entries)
        while low < high:
            mid = (low + high) // 2
            if entries[mid][0] < key:
                low = mid + 1
            else:
                high = mid

        return entries[:low] + [new_entry] + entries[low:]

    def _split_leaf_node(self, page, temp_entries, cursor):
        split_idx = len(temp_entries) // 2
        promoted_key = temp_entries[split_idx][0]

        # Dividir entradas
        left_entries = temp_entries[:split_idx]
        right_entries = temp_entries[split_idx:]

        new_leaf = LeafPage(
            page_id=cursor.total_blocks(),
            parent_id=page.parent_id,  # Padre temporal
            key_value_pairs=right_entries,
            next_leaf=page.next_leaf
        )

        # Actualizar hoja original
        page.next_leaf = new_leaf.page_id
        page.key_value_pairs = left_entries
        page.num_keys = len(left_entries)

        # Escribir cambios
        cursor.update_block(page.page_id, page.pack())
        cursor.append_block(new_leaf.pack())

        return promoted_key, new_leaf

    def _split_internal_node(self, page, cursor):
        split_idx = len(page.keys) // 2
        promoted_key = page.keys[split_idx]

        new_internal = InternalPage(
            page_id=cursor.total_blocks(),
            parent_id=page.parent_id,
            keys=page.keys[split_idx+1:],
            pointers=page.pointers[split_idx+1:]
        )

        # actualizar page original
        page.keys = page.keys[:split_idx]
        page.pointers = page.pointers[:split_idx+1]
        page.num_keys = len(page.keys)

        cursor.update_block(page.page_id, page.pack()) #sobreescribir
        cursor.append_block(new_internal.pack()) #al final del archivo
        return promoted_key, new_internal

    def _update_root_block(self, new_root_block):
        with BlockCursor(self.index_filename, PAGE_SIZE) as cursor:
            header_data = struct.pack(HEADER_FORMAT, new_root_block).ljust(PAGE_SIZE, b'\x00')
            cursor.update_block(0, header_data)

    def _create_new_root(self, K, left_ptr, right_ptr, cursor):
        new_root_id = cursor.total_blocks()
        new_root = InternalPage(
            page_id=new_root_id,
            parent_id=-1,
            keys=[K],
            pointers=[left_ptr, right_ptr]
        )

        # Actualizar padres de los hijos
        for ptr in [left_ptr, right_ptr]:
            child_data = cursor.read_block(ptr)
            child = self._parse_page(child_data)
            child.parent_id = new_root_id
            cursor.update_block(ptr, child.pack())

        cursor.append_block(new_root.pack())
        self._update_root_block(new_root_id)  
        self.root_block = new_root_id  # actualizar root en objeto

    def _propagate_split(self, stack, K, left_ptr, right_ptr, cursor):
        while stack:
            parent_block = stack.pop()
            parent = self._parse_page(cursor.read_block(parent_block)) #data del header

            if parent is None:
                return None

            assert isinstance(parent, InternalPage) #para que no llore el linter
            # insertar al padre
            insert_pos = bisect.bisect_right(parent.keys, K)

            parent.keys.insert(insert_pos, K)
            parent.pointers.insert(insert_pos + 1, right_ptr)
            parent.num_keys += 1

            if parent.num_keys <= self.internal_capacity:
                cursor.update_block(parent_block, parent.pack())
                return

            # split interno si fuera necesario
            promoted_key, new_internal = self._split_internal_node(parent, cursor)

            # actualizar referencias al padre en los hijos
            for ptr in new_internal.pointers:
                child = self._parse_page(cursor.read_block(ptr))
                if child is None:
                    return None

                assert isinstance(child, BPlusPage)
                child.parent_id = new_internal.page_id
                cursor.update_block(ptr, child.pack())

            K = promoted_key
            right_ptr = new_internal.page_id
            left_ptr = parent_block

        # Crear nueva raíz si es necesario
        self._create_new_root(K, left_ptr, right_ptr, cursor)

    def _insert_entry(self, key, ptr):
        """Insert a new entry into the tree"""
        stack = []
        with BlockCursor(self.index_filename, PAGE_SIZE) as cursor:
            page = self._find_leaf_page(cursor, key, stack)

            # verificar duplicados
            if any(k == key for k, _ in page.key_value_pairs):
                raise ValueError(f"Key {key} already exists")

            # insertar en la hoja
            temp_entries = self._insert_into_temp_entries(page.key_value_pairs, key, ptr)

            if len(temp_entries) <= self.leaf_capacity: #actualizamos sobre el mismo page
                self._update_leaf(page, temp_entries, cursor)
                return

            # split en la hoja y propagar hacia arriba
            promoted_key, new_leaf = self._split_leaf_node(page, temp_entries, cursor)
            self._propagate_split(stack, promoted_key, page.page_id, new_leaf.page_id, cursor)

    def _extract_key(self, data):
        """Extract key from raw data - assumes key is already in numeric format"""
        unpacked = struct.unpack(self.data_format, data)
        return unpacked[self.key_position]  # Assume caller has converted to numeric format
        
    def add(self, key, ptr):
        """Add a new key-pointer pair to the index"""
        # Convert pointer to signed long long range if needed
        if isinstance(ptr, int):
            ptr = max(-9223372036854775808, min(ptr, 9223372036854775807))
        self._insert_entry(key, ptr)

    def remove(self):
        pass

    def get_record(self, ptr):
        """Obtiene el registro RAW del datafile"""
        table_info = {
            "data_file": self.data_filename,
            "format_str": self.data_format
        }
        with LineCursor(table_info) as lc:
            lc.goto_line(ptr)
            return lc.file.read(self.record_size)  # Return raw bytes

    def print_tree_structure(self):
        with BlockCursor(self.index_filename, PAGE_SIZE) as cursor:
            print("\n=== ESTRUCTURA DEL ÁRBOL ===")
            queue = [(self.root_block, 0)]
            while queue:
                block_id, level = queue.pop(0)
                data = cursor.read_block(block_id)
                if not data:
                    continue  # Saltar bloques no existentes
                page = self._parse_page(data)
                if page is None:
                    continue
                if page.is_leaf:
                    leaf = page
                    keys = [k for k, _ in leaf.key_value_pairs]
                    print(f"Hoja {leaf.page_id} (padre: {leaf.parent_id}) -> Claves: {keys} | Siguiente: {leaf.next_leaf}")
                else:
                    internal = page
                    print(f"Nodo Interno {internal.page_id} (padre: {internal.parent_id}) -> Claves: {internal.keys} | Punteros: {internal.pointers}")
                    # Asegurar que todos los punteros se agreguen
                    queue.extend([(ptr, level+1) for ptr in page.pointers if ptr != -1])

    def debug_search(self, key):
        print(f"\n*** Rastreando clave {key} ***")
        with BlockCursor(self.index_filename, PAGE_SIZE) as cursor:
            current_block = self.root_block
            while True:
                data = cursor.read_block(current_block)
                page = self._parse_page(data)
                print(f"Bloque {current_block} ({'Hoja' if page.is_leaf else 'Interno'}): Claves={page.keys if not page.is_leaf else [k for k, _ in page.key_value_pairs]}")
                if page.is_leaf:
                    break
                else:
                    idx = bisect.bisect_right(page.keys, key)
                    print(f"Enrutando a puntero[{idx}]: {page.pointers[idx]}")
                    current_block = page.pointers[idx]
        print("--- Fin del rastreo ---")
