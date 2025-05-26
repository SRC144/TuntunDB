from db.cursors.block_cursor import BlockCursor
import struct

class ExtendibleHashingIndex:
    DIRECTORY_FILE = "dir.idx"
    BUCKET_FILE = "buckets.dat"
    POINTER_SIZE = 4  # bytes per bucket pointer
    DEPTH_SIZE = 1    # bytes for directory depth or bucket local depth

    def _init_(self, block_size=4096, bucket_capacity=100):
        self.block_size = block_size
        self.bucket_capacity = bucket_capacity
        # cursors
        self.dir_cursor = BlockCursor(self.DIRECTORY_FILE, self.block_size)
        self.bucket_cursor = BlockCursor(self.BUCKET_FILE, self.block_size)
        # initialize if empty
        if self.dir_cursor.total_blocks() == 0:
            self._init_directory()

    def _init_directory(self):
        # initial global depth
        depth = 1
        # header block for directory: depth
        header = struct.pack("B", depth).ljust(self.block_size, b"\x00")
        self.dir_cursor.append_block(header)
        # create two initial buckets
        buckets = []
        for _ in range(2):
            buckets.append(self._new_bucket(depth))
        # directory pointers block
        ptrs = b"".join(struct.pack("I", bnum) for bnum in buckets)
        dir_block = ptrs.ljust(self.block_size, b"\x00")
        self.dir_cursor.append_block(dir_block)

    def _new_bucket(self, local_depth):
        # create bucket block with local depth and zero count
        data = struct.pack("B", local_depth) + struct.pack("I", 0)
        data = data.ljust(self.block_size, b"\x00")
        self.bucket_cursor.append_block(data)
        return self.bucket_cursor.current_block_number() - 1

    def _hash(self, key):
        return hash(key) & 0xFFFFFFFF

    def _get_directory(self):
        self.dir_cursor.goto_block(0)
        header = self.dir_cursor.read()
        depth = struct.unpack("B", header[:1])[0]
        
        self.dir_cursor.goto_block(1)
        data = self.dir_cursor.read()
        ptrs = [struct.unpack("I", data[i*4:(i+1)*4])[0] for i in range(2*depth)]
        return depth, ptrs

    def _save_directory(self, depth, ptrs):
        header = struct.pack("B", depth).ljust(self.block_size, b"\x00")
        self.dir_cursor.goto_block(0)
        self.dir_cursor.overwrite_current(header)
        blk = b"".join(struct.pack("I", b) for b in ptrs).ljust(self.block_size, b"\x00")
        self.dir_cursor.goto_block(1)
        self.dir_cursor.overwrite_current(blk)

    def _read_bucket(self, block_num):
        data = self.bucket_cursor.read_block(block_num)
        local_depth = struct.unpack("B", data[:1])[0]
        cnt = struct.unpack("I", data[1:5])[0]
        entries = []
        off = 5
        for _ in range(cnt):
            entries.append(struct.unpack("I", data[off:off+4])[0])
            off += 4
        return local_depth, entries

    def _write_bucket(self, block_num, local_depth, entries):
        cnt = len(entries)
        data = struct.pack("B", local_depth) + struct.pack("I", cnt)
        for k in entries:
            data += struct.pack("I", k)
        data = data.ljust(self.block_size, b"\x00")
        self.bucket_cursor.update_block(block_num, data)

    def search(self, key):
        h = self._hash(key)
        depth, ptrs = self._get_directory()
        idx = h >> (32 - depth)
        blk = ptrs[idx]
        _, entries = self._read_bucket(blk)
        return h in entries

    def add(self, key):
        h = self._hash(key)
        if self.search(key):
            return False
        depth, ptrs = self._get_directory()
        idx = h >> (32 - depth)
        blk = ptrs[idx]
        local_depth, entries = self._read_bucket(blk)
        if len(entries) < self.bucket_capacity:
            entries.append(h)
            self._write_bucket(blk, local_depth, entries)
            return True
        # need split
        return self._split_and_insert(depth, ptrs, idx, h)

    def _split_and_insert(self, depth, ptrs, idx, h):
        blk = ptrs[idx]
        local_depth, entries = self._read_bucket(blk)
        # if local depth equals global, double directory
        if local_depth == depth:
            depth += 1
            ptrs = ptrs * 2
        # increment bucket local depth
        new_local = local_depth + 1
        # create sibling bucket
        sibling = self._new_bucket(new_local)
        # redistribute
        entries.append(h)
        mask = 1 << (32 - new_local)
        b0, b1 = [], []
        for k in entries:
            (b1 if (k & mask) else b0).append(k)
        self._write_bucket(blk, new_local, b0)
        self._write_bucket(sibling, new_local, b1)
        # update directory pointers
        prefix = idx >> (depth - new_local)
        shift = depth - new_local - 1
        if shift < 0:
            shift = 0
        for i in range(len(ptrs)):
            if i >> (depth - new_local) == prefix:
                ptrs[i] = sibling if (i & (1 << shift)) else blk
        # save directory
        self._save_directory(depth, ptrs)
        return True

    def remove(self, key):
        h = self._hash(key)
        depth, ptrs = self._get_directory()
        idx = h >> (32 - depth)
        blk = ptrs[idx]
        ld, entries = self._read_bucket(blk)
        if h not in entries:
            return False
        entries.remove(h)
        self._write_bucket(blk, ld, entries)
        return True