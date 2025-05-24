from implementations import (
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
    def get_index(cls, index_type, data_file=None, **kwargs):
        #Validar tipo de índice
        index_class = cls._INDEX_CLASSES.get(index_type)
        if not index_class:
            raise ValueError(f"Tipo de índice no válido: {index_type}")

        #Crear instancia
        if data_file:
            return index_class.create_from_data(data_file, **kwargs)
        else:
            return index_class(**kwargs)
