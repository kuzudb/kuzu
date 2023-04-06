from . import _kuzu
from .types import Type


class Database:
    """
    Kùzu database instance.

    Methods
    -------
    resize_buffer_manager(new_size)
        Resize the mamimum size of buffer pool.

    set_logging_level(level)
        Set the logging level.

    get_torch_geometric_remote_backend(num_threads)
        Get the torch_geometric remote backend.

    """

    def __init__(self, database_path, buffer_pool_size=0, lazy_init=False):
        """
        Parameters
        ----------
        database_path : str
            The path to database files
        buffer_pool_size : int
            The maximum size of buffer pool in bytes (Optional). Default to 80% 
            of system memory.
        lazy_init : bool
            If True, the database will not be initialized until the first query.
            This is useful when the database is not used in the main thread or 
            when the main process is forked.
            Deefault to False.
        """
        self.database_path = database_path
        self.buffer_pool_size = buffer_pool_size
        self._database = None
        if not lazy_init:
            self.init_database()

    def __getstate__(self):
        state = {
            "database_path": self.database_path,
            "buffer_pool_size": self.buffer_pool_size,
            "database": None
        }
        return state

    def init_database(self):
        """
        Initialize the database.
        """
        if self._database is None:
            self._database = _kuzu.Database(self.database_path,
                                            self.buffer_pool_size)

    def resize_buffer_manager(self, new_size):
        """
        Resize the mamimum size of buffer pool.

        Parameters
        ----------
        new_size : int
            New maximum size of buffer pool (in bytes).
        """

        self._database.resize_buffer_manager(new_size)

    def set_logging_level(self, level):
        """
        Set the logging level.

        Parameters
        ----------
        level : str
            Logging level. One of "debug", "info", "err".
        """

        self._database.set_logging_level(level)

    def get_torch_geometric_remote_backend(self, num_threads=None):
        from .torch_geometric_feature_store import KuzuFeatureStore
        from .torch_geometric_graph_store import KuzuGraphStore
        """
        Get the torch_geometric remote backend.

        Parameters
        ----------
        num_threads : int
            Number of threads to use for data loading. Default to None, which
            means using the number of CPU cores.

        Returns
        -------
        feature_store : KuzuFeatureStore
            Feature store compatible with torch_geometric.
        graph_store : KuzuGraphStore
            Graph store compatible with torch_geometric.
        """
        return KuzuFeatureStore(self, num_threads), KuzuGraphStore(self, num_threads)

    def _scan_node_table(self, table_name, prop_name, prop_type, dim, indices, num_threads):
        import numpy as np
        """
        Scan a node table from storage directly, bypassing query engine.
        Used internally by torch_geometric remote backend only.
        """
        self.init_database()
        indices_cast = np.array(indices, dtype=np.uint64)

        result = None
        if prop_type == Type.INT64.value:
            result = np.empty(len(indices) * dim, dtype=np.int64)
            self._database.scan_node_table_as_int64(
                table_name, prop_name, indices_cast, result, num_threads)
        if prop_type == Type.INT32.value:
            result = np.empty(len(indices) * dim, dtype=np.int32)
            self._database.scan_node_table_as_int32(
                table_name, prop_name, indices_cast, result, num_threads)
        if prop_type == Type.INT16.value:
            result = np.empty(len(indices) * dim, dtype=np.int16)
            self._database.scan_node_table_as_int16(
                table_name, prop_name, indices_cast, result, num_threads)
        if prop_type == Type.DOUBLE.value:
            result = np.empty(len(indices) * dim, dtype=np.float64)
            self._database.scan_node_table_as_double(
                table_name, prop_name, indices_cast, result, num_threads)
        if prop_type == Type.FLOAT.value:
            result = np.empty(len(indices) * dim, dtype=np.float32)
            self._database.scan_node_table_as_float(
                table_name, prop_name, indices_cast, result, num_threads)
        if result is not None:
            return result
        raise ValueError("Unsupported property type: {}".format(prop_type))
