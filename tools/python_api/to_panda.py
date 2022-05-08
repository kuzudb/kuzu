from tools.python_api import _graphflowdb as gdb
import pandas as pd


def get_target_cols(result, target_col_ids):
    target_cols = []
    for target_col_id in target_col_ids:
        target_cols.append(result[target_col_id])
    return target_cols


def get_target_col_ids(name_list, col_names, is_node_name=True):
    target_col_ids = []
    for name in name_list:
        for i in range(0, len(col_names)):
            if is_node_name and col_names[i].startswith(name):
                target_col_ids.append(i)
            elif not is_node_name and col_names[i] == name:
                target_col_ids.append(i)
    return target_col_ids


def to_panda(db, query, **kwargs):
    conn = gdb.connection(db)
    result = conn.execute(query)
    col_names = result.getColumnNames()
    num_cols = len(col_names)
    target_col_ids = []
    if "node_name" in kwargs:
        target_col_ids.extend(get_target_col_ids(kwargs["node_name"], col_names))
    if "relation_name" in kwargs:
        target_col_ids.extend(get_target_col_ids(kwargs["relation_name"], col_names))
    if "col_name" in kwargs:
        target_col_ids.extend(get_target_col_ids(kwargs["col_name"], col_names))
    if not target_col_ids:
        target_col_ids = list(range(0, num_cols))
    target_col_names = []
    for target_col_id in target_col_ids:
        target_col_names.append(col_names[target_col_id])
    table_content = []
    while result.hasNext():
        table_content.append(get_target_cols(result.getNext(), target_col_ids))

    result.close()
    df = pd.DataFrame(table_content)
    df.columns = target_col_names
    return df
