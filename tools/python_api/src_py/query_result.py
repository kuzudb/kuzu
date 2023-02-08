from .torch_geometric_result_converter import TorchGeometricResultConverter
from .types import Type


class QueryResult:
    """
    Query result.

    Methods
    -------
    check_for_query_result_close()
        Check if the query result is closed and raise an exception if it is.

    has_next()
        Check if there are more rows in the query result.

    get_next()
        Get the next row in the query result.

    write_to_csv(filename, delimiter=',', escape_character='"', newline='\n')
        Write the query result to a CSV file.

    close()
        Close the query result.
    
    get_as_df()
        Get the query result as a Pandas DataFrame.

    get_as_arrow(chunk_size)
        Get the query result as a PyArrow Table.

    get_column_data_types()
        Get the data types of the columns in the query result.

    get_column_names()
        Get the names of the columns in the query result.

    get_as_torch_geometric()
        Get the query result as a PyTorch Geometric Data object.
    """
    
    def __init__(self, connection, query_result):
        """
        Parameters
        ----------
        connection : _kuzu.Connection
            Connection to the database.
        query_result : _kuzu.QueryResult
            Query result.
        """

        self.connection = connection
        self._query_result = query_result
        self.is_closed = False

    def __del__(self):
        self.close()

    def check_for_query_result_close(self):
        """
        Check if the query result is closed and raise an exception if it is.

        Raises
        ------
        Exception
            If the query result is closed.
        """

        if self.is_closed:
            raise Exception("Query result is closed")

    def has_next(self):
        """
        Check if there are more rows in the query result.

        Returns
        -------
        bool
            True if there are more rows in the query result, False otherwise.
        """

        self.check_for_query_result_close()
        return self._query_result.hasNext()

    def get_next(self):
        """
        Get the next row in the query result.

        Returns
        -------
        list
            Next row in the query result.
        """

        self.check_for_query_result_close()
        return self._query_result.getNext()

    def write_to_csv(self, filename, delimiter=',', escape_character='"', newline='\n'):
        """
        Write the query result to a CSV file.

        Parameters
        ----------
        filename : str
            Name of the CSV file to write to.
        delimiter : str
            Delimiter to use in the CSV file. Defaults to ','.
        escape_character : str
            Escape character to use in the CSV file. Defaults to '"'.
        newline : str
            Newline character to use in the CSV file. Defaults to '\n'.
        """

        self.check_for_query_result_close()
        self._query_result.writeToCSV(
            filename, delimiter, escape_character, newline)

    def close(self):
        """
        Close the query result.
        """

        if self.is_closed:
            return
        self._query_result.close()
        # Allows the connection to be garbage collected if the query result
        # is closed manually by the user.
        self.connection = None
        self.is_closed = True

    def get_as_df(self):
        """
        Get the query result as a Pandas DataFrame.

        Returns
        -------
        pandas.DataFrame
            Query result as a Pandas DataFrame.
        """

        self.check_for_query_result_close()
        import numpy
        import pandas

        return self._query_result.getAsDF()

    def get_as_arrow(self, chunk_size):
        """
        Get the query result as a PyArrow Table.

        Parameters
        ----------
        chunk_size : int
            Number of rows to include in each chunk.

        Returns
        -------
        pyarrow.Table
            Query result as a PyArrow Table.
        """

        self.check_for_query_result_close()
        import pyarrow

        return self._query_result.getAsArrow(chunk_size)

    def get_column_data_types(self):
        """
        Get the data types of the columns in the query result.

        Returns
        -------
        list
            Data types of the columns in the query result.
        """

        self.check_for_query_result_close()
        return self._query_result.getColumnDataTypes()

    def get_column_names(self):
        """
        Get the names of the columns in the query result.

        Returns
        -------
        list
            Names of the columns in the query result.
        """

        self.check_for_query_result_close()
        return self._query_result.getColumnNames()

    def reset_iterator(self):
        """
        Reset the iterator of the query result.
        """

        self.check_for_query_result_close()
        self._query_result.resetIterator()

    def get_as_networkx(self, directed=True):
        """
        Get the query result as a NetworkX graph.

        Parameters
        ----------
        directed : bool
            Whether the graph should be directed. Defaults to True.
        
        Returns
        -------
        networkx.DiGraph or networkx.Graph
            Query result as a NetworkX graph.
        """
        
        self.check_for_query_result_close()
        import networkx as nx

        if directed:
            nx_graph = nx.DiGraph()
        else:
            nx_graph = nx.Graph()
        properties_to_extract = self._get_properties_to_extract()

        self.reset_iterator()

        nodes = {}
        rels = {}
        table_to_label_dict = {}
        table_primary_key_dict = {}

        def encode_node_id(node, table_primary_key_dict):
            return node['_label'] + "_" + str(node[table_primary_key_dict[node['_label']]])

        # De-duplicate nodes and rels
        while self.has_next():
            row = self.get_next()
            for i in properties_to_extract:
                column_type, _ = properties_to_extract[i]
                if column_type == Type.NODE.value:
                    _id = row[i]["_id"]
                    nodes[(_id["table"], _id["offset"])] = row[i]
                    table_to_label_dict[_id["table"]] = row[i]["_label"]

                elif column_type == Type.REL.value:
                    _src = row[i]["_src"]
                    _dst = row[i]["_dst"]
                    rels[(_src["table"], _src["offset"], _dst["table"],
                          _dst["offset"])] = row[i]

        # Add nodes
        for node in nodes.values():
            _id = node["_id"]
            node_id = node['_label'] + "_" + str(_id["offset"])
            if node['_label'] not in table_primary_key_dict:
                props = self.connection._get_node_property_names(node['_label'])
                for prop_name in props:
                    if props[prop_name]['is_primary_key']:
                        table_primary_key_dict[node['_label']] = prop_name
                        break
            node_id = encode_node_id(node, table_primary_key_dict)
            node[node['_label']] = True
            nx_graph.add_node(node_id, **node)

        # Add rels
        for rel in rels.values():
            _src = rel["_src"]
            _dst = rel["_dst"]
            src_node = nodes[(_src["table"], _src["offset"])]
            dst_node = nodes[(_dst["table"], _dst["offset"])]
            src_id = encode_node_id(src_node, table_primary_key_dict)
            dst_id = encode_node_id(dst_node, table_primary_key_dict)
            nx_graph.add_edge(src_id, dst_id, **rel)
        return nx_graph

    def _get_properties_to_extract(self):
        column_names = self.get_column_names()
        column_types = self.get_column_data_types()
        properties_to_extract = {}

        # Iterate over columns and extract nodes and rels, ignoring other columns
        for i in range(len(column_names)):
            column_name = column_names[i]
            column_type = column_types[i]
            if column_type in [Type.NODE.value, Type.REL.value]:
                properties_to_extract[i] = (column_type, column_name)
        return properties_to_extract

    def get_as_torch_geometric(self):
        """
        Get the query result as a PyTorch Geometric graph.

        Returns
        -------
        torch_geometric.data.Data or torch_geometric.data.HeteroData
            Query result as a PyTorch Geometric graph.
        """

        self.check_for_query_result_close()
        # Despite we are not using torch_geometric in this file, we need to
        # import it here to throw an error early if the user does not have
        # torch_geometric or torch installed.

        import torch
        import torch_geometric

        converter = TorchGeometricResultConverter(self)
        return converter.get_as_torch_geometric()
