from .implementations import (
    SequentialFileIndex,
    ISAMSparseIndex,
    ExtendibleHashingIndex,
    BPlusTreeIndex,
    RTreeIndex
)

class IndexFactory:
    # Mapeo de nombres a clases
    _INDEX_CLASSES = {
        "sequential": SequentialFileIndex,
        "isam": ISAMSparseIndex,
        "hash": ExtendibleHashingIndex,
        "bplus": BPlusTreeIndex,
        "rtree": RTreeIndex
    }

    @classmethod
    def get_index(cls, index_type, index_filename, data_filename=None, data_format=None, key_position=0, **kwargs):
        """
        Create or get an index instance.
        
        Args:
            index_type: Type of index to create ('sequential', 'isam', 'hash', 'bplus', 'rtree')
            index_filename: Path to the index file
            data_filename: Path to the data file (optional)
            data_format: Struct format string for the data (optional)
            key_position: Position of the key in the record (optional)
            **kwargs: Additional arguments for specific index types
        """
        #Validar tipo de índice
        index_class = cls._INDEX_CLASSES.get(index_type.lower())
        if not index_class:
            raise ValueError(f"Tipo de índice no válido: {index_type}")

        #Crear instancia
        return index_class(
            index_filename=index_filename,
            data_filename=data_filename,
            data_format=data_format,
            key_position=key_position,
            **kwargs
        )
