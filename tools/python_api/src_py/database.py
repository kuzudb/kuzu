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
        Use the database as the remote backend for torch_geometric. 

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
            "_database": None
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
        """
        Use the database as the remote backend for torch_geometric. 

        For the interface of the remote backend, please refer to 
        https://pytorch-geometric.readthedocs.io/en/latest/advanced/remote.html.
        The current implementation is read-only and does not support edge
        features. The IDs of the nodes are based on the internal IDs (i.e., node
        offsets). For the remote node IDs to be consistent with the positions in
        the output tensors, please ensure that no deletion has been performed
        on the node tables. 

        The remote backend can also be plugged into the data loader of 
        torch_geometric, which is useful for mini-batch training. For example:

        .. code-block:: python

            loader_kuzu = NeighborLoader(
                data=(feature_store, graph_store),
                num_neighbors={('paper', 'cites', 'paper'): [12, 12, 12]},
                batch_size=LOADER_BATCH_SIZE,
                input_nodes=('paper', input_nodes),
                num_workers=4,
                filter_per_worker=False,
            )
        
        Please note that the database instance is not fork-safe, so if more than
        one worker is used, `filter_per_worker` must be set to False.

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
        from .torch_geometric_feature_store import KuzuFeatureStore
        from .torch_geometric_graph_store import KuzuGraphStore
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
