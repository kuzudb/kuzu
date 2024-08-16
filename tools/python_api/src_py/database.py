from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import _kuzu
from .types import Type

if TYPE_CHECKING:
    import sys
    from types import TracebackType

    from numpy.typing import NDArray
    from torch_geometric.data.feature_store import IndexType

    from .torch_geometric_feature_store import KuzuFeatureStore
    from .torch_geometric_graph_store import KuzuGraphStore

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self


class Database:
    """Kùzu database instance."""

    def __init__(
        self,
        database_path: str | Path | None = None,
        *,
        buffer_pool_size: int = 0,
        max_num_threads: int = 0,
        compression: bool = True,
        lazy_init: bool = False,
        read_only: bool = False,
        max_db_size: int = (1 << 43),
    ):
        """
        Parameters
        ----------
        database_path : str, Path
            The path to database files. If the path is not specified, or empty, or equal to `:memory:`, the database
            will be created in memory.

        buffer_pool_size : int
            The maximum size of buffer pool in bytes. Defaults to ~80% of system memory.

        max_num_threads : int
            The maximum number of threads to use for executing queries.

        compression : bool
            Enable database compression.

        lazy_init : bool
            If True, the database will not be initialized until the first query.
            This is useful when the database is not used in the main thread or
            when the main process is forked.
            Default to False.

        read_only : bool
            If true, the database is opened read-only. No write transactions is
            allowed on the `Database` object. Multiple read-only `Database`
            objects can be created with the same database path. However, there
            cannot be multiple `Database` objects created with the same
            database path.
            Default to False.

        max_db_size : int
            The maximum size of the database in bytes. Note that this is introduced
            temporarily for now to get around with the default 8TB mmap address
             space limit some environment. This will be removed once we implemente
             a better solution later. The value is default to 1 << 43 (8TB) under 64-bit
             environment and 1GB under 32-bit one.

        """
        if database_path is None:
            database_path = ":memory:"
        if isinstance(database_path, Path):
            database_path = str(database_path)

        self.database_path = database_path
        self.buffer_pool_size = buffer_pool_size
        self.max_num_threads = max_num_threads
        self.compression = compression
        self.read_only = read_only
        self.max_db_size = max_db_size
        self.is_closed = False

        self._database: Any = None  # (type: _kuzu.Database from pybind11)
        if not lazy_init:
            self.init_database()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    @staticmethod
    def get_version() -> str:
        """
        Get the version of the database.

        Returns
        -------
        str
            The version of the database.
        """
        return _kuzu.Database.get_version()  # type: ignore[union-attr]

    @staticmethod
    def get_storage_version() -> int:
        """
        Get the storage version of the database.

        Returns
        -------
        int
            The storage version of the database.
        """
        return _kuzu.Database.get_storage_version()  # type: ignore[union-attr]

    def __getstate__(self) -> dict[str, Any]:
        state = {
            "database_path": self.database_path,
            "buffer_pool_size": self.buffer_pool_size,
            "compression": self.compression,
            "read_only": self.read_only,
            "_database": None,
        }
        return state

    def init_database(self) -> None:
        """Initialize the database."""
        self.check_for_database_close()
        if self._database is None:
            self._database = _kuzu.Database(  # type: ignore[union-attr]
                self.database_path,
                self.buffer_pool_size,
                self.max_num_threads,
                self.compression,
                self.read_only,
                self.max_db_size,
            )

    def get_torch_geometric_remote_backend(
        self, num_threads: int | None = None
    ) -> tuple[KuzuFeatureStore, KuzuGraphStore]:
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

        ```python
            loader_kuzu = NeighborLoader(
                data=(feature_store, graph_store),
                num_neighbors={('paper', 'cites', 'paper'): [12, 12, 12]},
                batch_size=LOADER_BATCH_SIZE,
                input_nodes=('paper', input_nodes),
                num_workers=4,
                filter_per_worker=False,
            )
        ```

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
        self.check_for_database_close()
        from .torch_geometric_feature_store import KuzuFeatureStore
        from .torch_geometric_graph_store import KuzuGraphStore

        return (
            KuzuFeatureStore(self, num_threads),
            KuzuGraphStore(self, num_threads),
        )

    def _scan_node_table(
        self,
        table_name: str,
        prop_name: str,
        prop_type: str,
        dim: int,
        indices: IndexType,
        num_threads: int,
    ) -> NDArray[Any]:
        self.check_for_database_close()
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
            self._database.scan_node_table_as_int64(table_name, prop_name, indices_cast, result, num_threads)
        elif prop_type == Type.INT32.value:
            result = np.empty(len(indices) * dim, dtype=np.int32)
            self._database.scan_node_table_as_int32(table_name, prop_name, indices_cast, result, num_threads)
        elif prop_type == Type.INT16.value:
            result = np.empty(len(indices) * dim, dtype=np.int16)
            self._database.scan_node_table_as_int16(table_name, prop_name, indices_cast, result, num_threads)
        elif prop_type == Type.DOUBLE.value:
            result = np.empty(len(indices) * dim, dtype=np.float64)
            self._database.scan_node_table_as_double(table_name, prop_name, indices_cast, result, num_threads)
        elif prop_type == Type.FLOAT.value:
            result = np.empty(len(indices) * dim, dtype=np.float32)
            self._database.scan_node_table_as_float(table_name, prop_name, indices_cast, result, num_threads)

        if result is not None:
            return result

        msg = f"Unsupported property type: {prop_type}"
        raise ValueError(msg)

    def close(self) -> None:
        """
        Close the database. Once the database is closed, the lock on the database
        files is released and the database can be opened in another process.

        Note: Call to this method is not required. The Python garbage collector
        will automatically close the database when no references to the database
        object exist. It is recommended not to call this method explicitly. If you
        decide to manually close the database, make sure that all the QueryResult
        and Connection objects are closed before calling this method.
        """
        if self.is_closed:
            return
        self.is_closed = True
        if self._database is not None:
            self._database.close()
            self._database: Any = None  # (type: _kuzu.Database from pybind11)

    def check_for_database_close(self) -> None:
        """
        Check if the database is closed and raise an exception if it is.

        Raises
        ------
        Exception
            If the database is closed.

        """
        if not self.is_closed:
            return
        msg = "Database is closed"
        raise RuntimeError(msg)
