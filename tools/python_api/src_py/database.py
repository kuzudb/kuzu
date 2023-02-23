from . import _kuzu
from .torch_geometric_feature_store import KuzuFeatureStore
from .torch_geometric_graph_store import KuzuGraphStore
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

    def get_torch_geometric_remote_backend(self):
        """
        Get the torch_geometric remote backend.

        Returns
        -------
        feature_store : KuzuFeatureStore
            Feature store compatible with torch_geometric.
        graph_store : KuzuGraphStore
            Graph store compatible with torch_geometric.
        """
        duplicated_db = Database(
            self.database_path, self.buffer_pool_size, True)
        return KuzuFeatureStore(duplicated_db), KuzuGraphStore(duplicated_db)

    def _scan_node_table(self, table_name, prop_name, prop_type, indices):
        """
        Scan a node table from storage directly, bypassing query engine.
        Used internally by torch_geometric remote backend only.
        """
        self.init_database()
        if prop_type == Type.INT64.value:
            return self._database.scan_node_table_as_int64(
                table_name, prop_name, indices)
        if prop_type == Type.DOUBLE.value:
            return self._database.scan_node_table_as_double(
                table_name, prop_name, indices)
        if prop_type == Type.BOOL.value:
            return self._database.scan_node_table_as_bool(
                table_name, prop_name, indices)
        raise ValueError("Unsupported property type: {}".format(prop_type))
