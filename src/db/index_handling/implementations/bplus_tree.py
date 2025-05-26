import struct
import os
import bisect
from ...cursors.line_cursor import LineCursor
from ...cursors import BlockCursor

# Constants for B+ tree
PAGE_SIZE = 4096  # 4KB pages
KEY_SIZE = 8  # 8 bytes for key (uint64)
PTR_SIZE = 8  # 8 bytes for pointer (signed long long)
PAGE_HEADER_SIZE = 15  # is_leaf(1) + num_keys(2) + page_id(4) + parent_id(8)
HEADER_SIZE = 8  # root_block(8)

# Format strings
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

class InternalPage(BPlusPage):
    def __init__(self, page_id, parent_id, keys, pointers):
        super().__init__(is_leaf=0, num_keys=len(keys), page_id=page_id, parent_id=parent_id)
        self.keys = keys  # List of raw key bytes
        self.pointers = pointers

    def pack(self):
        """Pack page into bytes"""
        data = self.header_bytes()
        
        # Pack keys and pointers - keys are already raw bytes
        for key, ptr in zip(self.keys, self.pointers):
            data += key  # Key is already raw bytes
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
            # Read key as raw bytes
            key = data[offset:offset + KEY_SIZE]
            offset += KEY_SIZE
            # Read pointer
            ptr = struct.unpack(PTR_FORMAT, data[offset:offset + PTR_SIZE])[0]
            keys.append(key)
            pointers.append(ptr)
            offset += PTR_SIZE
            
        # Read last pointer
        last_ptr = struct.unpack(PTR_FORMAT, data[offset:offset + PTR_SIZE])[0]
        pointers.append(last_ptr)

        return cls(page_id, parent_id, keys, pointers)

class LeafPage(BPlusPage):
    def __init__(self, page_id, parent_id, key_value_pairs, next_leaf):
        super().__init__(is_leaf=1, num_keys=len(key_value_pairs), page_id=page_id, parent_id=parent_id)
        self.key_value_pairs = key_value_pairs  # List of (key_bytes, ptr) tuples
        self.next_leaf = next_leaf

    def pack(self):
        """Pack page into bytes"""
        data = self.header_bytes()
        
        # Pack key-value pairs - keys are already raw bytes
        for key, ptr in self.key_value_pairs:
            data += key  # Key is already raw bytes
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
            # Read key as raw bytes
            key = data[offset:offset + KEY_SIZE]
            offset += KEY_SIZE
            # Read pointer
            ptr = struct.unpack(PTR_FORMAT, data[offset:offset + PTR_SIZE])[0]
            key_value_pairs.append((key, ptr))
            offset += PTR_SIZE

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

        # Don't build from data immediately since the data file might be empty
        # The index will be built when data is inserted

    def _init_storage(self):
        file_exists = os.path.exists(self.index_filename)
        if not file_exists:
            self.create_empty()
            return

        # File exists, check if it's properly initialized
        with BlockCursor(self.index_filename, PAGE_SIZE) as cursor:
            header_data = cursor.read_block(0)
        if header_data is None or len(header_data) == 0:
            # File exists but is empty, create new tree
            self.create_empty()
            return

        # Ensure header is properly formatted
        try:
            self.root_block = struct.unpack(HEADER_FORMAT, header_data[:HEADER_SIZE])[0]
        except struct.error:
            raise ValueError("Invalid header block format")

    def create_empty(self):
        """Create an empty B+ tree index structure"""
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
        """Build index from existing data file"""
        # Reset index on disk
        if os.path.exists(self.index_filename):
            os.remove(self.index_filename)
        self._init_storage()
        
        # Read data file and add each record
        try:
            with open(self.data_filename, 'rb') as f:
                while True:
                    raw = f.read(self.record_size)
                    if not raw or len(raw) < self.record_size:
                        break
                    self.add(raw)
        except Exception as e:
            raise ValueError(f"Failed to build index: {str(e)}")

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

        # Para duplicados, avanzar hasta el último elemento con la misma clave
        while low < len(entries) and entries[low][0] == key:
            low += 1

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

            # insertar en la hoja
            temp_entries = self._insert_into_temp_entries(page.key_value_pairs, key, ptr)

            if len(temp_entries) <= self.leaf_capacity: #actualizamos sobre el mismo page
                self._update_leaf(page, temp_entries, cursor)
                return

            # split en la hoja y propagar hacia arriba
            promoted_key, new_leaf = self._split_leaf_node(page, temp_entries, cursor)
            self._propagate_split(stack, promoted_key, page.page_id, new_leaf.page_id, cursor)

    def _extract_key(self, data):
        """Extract key from raw data - just get the raw bytes for the key"""
        try:
            unpacked = struct.unpack(self.data_format, data)
            key_bytes = unpacked[self.key_position]
            if isinstance(key_bytes, int):
                # Convert integer to bytes
                key_bytes = key_bytes.to_bytes(KEY_SIZE, byteorder='little')
            elif isinstance(key_bytes, str):
                # Convert string to bytes
                key_bytes = key_bytes.encode().ljust(KEY_SIZE, b'\x00')
            elif isinstance(key_bytes, bytes):
                # Already bytes, just ensure right size
                key_bytes = key_bytes[:KEY_SIZE].ljust(KEY_SIZE, b'\x00')
            else:
                raise ValueError(f"Unsupported key type: {type(key_bytes)}")
            return key_bytes
        except Exception as e:
            print(f"Error extracting key: {e}")
            raise

    def insert_key_and_position(self, key, position):
        """Insert a key and position into the index without writing to data file"""
        self._insert_entry(key, position)

    def _write_data_record(self, data):
        """Write data record to file and return its line number"""
        with LineCursor(self.data_filename, self.record_size) as lc:
            lc.goto_end()
            position = lc.current_record_number()
            lc.append_record(data)
            return position

    def add(self, data):
        """Insert a new record into the index and data"""
        if len(data) != self.record_size:
            raise ValueError(f"Data size must be {self.record_size} bytes, got {len(data)} bytes")

        try:
            key = self._extract_key(data)
            ptr = self._write_data_record(data)  # ptr is the line number
            self._insert_entry(key, ptr)
        except Exception as e:
            print(f"[Error in add method: {e}")
            raise

    def remove(self, key):
        """Elimina clave y reequilibra según algoritmo del paper."""
        with BlockCursor(self.index_filename, PAGE_SIZE) as c:
            # Descender hasta la hoja y guardar stack de (blk, page, idx)
            stack=[]
            blk=self.root_block

            while True:
                pg=self._parse_page(c.read_block(blk))
                if pg.is_leaf:
                    leaf, leaf_blk = pg, blk
                    break
                idx = bisect.bisect_right(pg.keys, key)
                stack.append((blk, pg, idx))
                blk = pg.pointers[idx]

            # Borrar en hoja
            for i,(k,p) in enumerate(leaf.key_value_pairs):
                if k==key:
                    leaf.key_value_pairs.pop(i)
                    leaf.num_keys-=1
                    c.update_block(leaf_blk, leaf.pack())
                    break
            else:
                return False
            # Reequilibrio
            self._delete_rebalance(c, leaf, leaf_blk, stack)
        return True

    def _delete_rebalance(self, c, node, blk, stack):
        # Umbrales mínimos
        min_leaf = (self.leaf_capacity+1)//2
        min_int  = (self.internal_capacity+1)//2

        # Caso root
        if node.parent_id < 0:
            if not node.is_leaf and node.num_keys==0:
                child_blk = node.pointers[0]
                child = self._parse_page(c.read_block(child_blk))
                if child:
                    child.parent_id = -1
                    c.update_block(child_blk, child.pack())
                    self._update_root_block(child_blk)
            return
        
        # Check underflow
        if (node.is_leaf and node.num_keys>=min_leaf) or (not node.is_leaf and node.num_keys>=min_int):
            return
        
        # Obtener padre info
        parent_blk, parent_pg, idx = stack.pop()
        # IDs de hermanos
        left_blk  = parent_pg.pointers[idx-1] if idx>0 else None
        right_blk = parent_pg.pointers[idx+1] if idx<parent_pg.num_keys else None

        # Leaf case
        if node.is_leaf:
            left = self._parse_page(c.read_block(left_blk)) if left_blk else None
            right= self._parse_page(c.read_block(right_blk)) if right_blk else None
            # Prestar de left
            if left and left.num_keys>min_leaf:
                k,p = left.key_value_pairs.pop(-1)
                node.key_value_pairs.insert(0,(k,p))
                left.num_keys-=1
                node.num_keys+=1
                parent_pg.keys[idx-1]=node.key_value_pairs[0][0]
                c.update_block(left_blk,left.pack())
                c.update_block(blk,node.pack())
                c.update_block(parent_blk,parent_pg.pack())
                return
            # Prestar de right
            if right and right.num_keys>min_leaf:
                k,p = right.key_value_pairs.pop(0)
                node.key_value_pairs.append((k,p))
                right.num_keys-=1
                node.num_keys+=1
                parent_pg.keys[idx]=right.key_value_pairs[0][0]
                c.update_block(right_blk,right.pack())
                c.update_block(blk,node.pack())
                c.update_block(parent_blk,parent_pg.pack())
                return
            # Merge
            if left:
                left.key_value_pairs += node.key_value_pairs
                left.next_leaf = node.next_leaf
                left.num_keys=len(left.key_value_pairs)
                c.update_block(left_blk,left.pack())
                parent_pg.keys.pop(idx-1)
                parent_pg.pointers.pop(idx)
            else:
                node.key_value_pairs += right.key_value_pairs
                node.next_leaf = right.next_leaf
                node.num_keys=len(node.key_value_pairs)
                c.update_block(blk,node.pack())
                parent_pg.keys.pop(idx)
                parent_pg.pointers.pop(idx+1)

            parent_pg.num_keys=len(parent_pg.keys)
            c.update_block(parent_blk,parent_pg.pack())
            # Recursión
            self._delete_rebalance(c, parent_pg, parent_blk, stack)
        else:
            # Internal node underflow
            left = self._parse_page(c.read_block(left_blk)) if left_blk else None
            right= self._parse_page(c.read_block(right_blk)) if right_blk else None
            # Borrow from left
            if left and left.num_keys>min_int:
                # move separator down
                sep = parent_pg.keys[idx-1]
                k = left.keys.pop(-1)
                p = left.pointers.pop(-1)
                node.keys.insert(0, sep)
                node.pointers.insert(0,p)
                parent_pg.keys[idx-1]=k
                left.num_keys-=1
                node.num_keys+=1

                # Actualizar padre del puntero movido
                child = self._parse_page(c.read_block(p))
                if child:
                    child.parent_id = blk
                    c.update_block(p, child.pack())
                c.update_block(left_blk,left.pack())
                c.update_block(blk,node.pack())
                c.update_block(parent_blk,parent_pg.pack())
                return
            # Borrow from right
            if right and right.num_keys>min_int:
                sep = parent_pg.keys[idx]
                k = right.keys.pop(0)
                p = right.pointers.pop(0)
                node.keys.append(sep)
                node.pointers.append(p)
                parent_pg.keys[idx]=k
                right.num_keys-=1
                node.num_keys+=1

                # Actualizar padre del puntero movido
                child = self._parse_page(c.read_block(p))
                if child:
                    child.parent_id = blk
                    c.update_block(p, child.pack())

                c.update_block(right_blk,right.pack())
                c.update_block(blk,node.pack())
                c.update_block(parent_blk,parent_pg.pack())
                return
            # Merge
            if left:
                # merge node into left
                sep = parent_pg.keys.pop(idx-1)
                left.keys.append(sep)
                left.keys += node.keys
                left.pointers += node.pointers
                left.num_keys=len(left.keys)
                # Actualizar padres de los punteros movidos
                for p in node.pointers:
                    child = self._parse_page(c.read_block(p))
                    if child:
                        child.parent_id = left_blk
                        c.update_block(p, child.pack())
                c.update_block(left_blk,left.pack())
                parent_pg.pointers.pop(idx)
            else:
                sep = parent_pg.keys.pop(idx)
                node.keys.append(sep)
                node.keys += right.keys
                node.pointers += right.pointers
                node.num_keys=len(node.keys)
                # Actualizar padres de los punteros movidos
                for p in right.pointers:
                    child = self._parse_page(c.read_block(p))
                    if child:
                        child.parent_id = blk
                        c.update_block(p, child.pack())
                c.update_block(blk,node.pack())
                parent_pg.pointers.pop(idx+1)
            parent_pg.num_keys=len(parent_pg.keys)
            c.update_block(parent_blk,parent_pg.pack())
            # Recursión
            self._delete_rebalance(c, parent_pg, parent_blk, stack)

    def get_record(self, ptr):
        """Obtiene el registro RAW del datafile"""
        with LineCursor(self.data_filename, self.record_size) as lc:
            return lc.read_at(ptr)  # Return raw bytes

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