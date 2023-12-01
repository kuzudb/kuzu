package com.kuzudb;

/**
 * KuzuInternalID type which stores the table_id and offset of a node/rel.
 */
public class KuzuInternalID {
    public long tableId;
    public long offset;

    /**
     * Create a KuzuInternalID from the given table_id and offset.
     * @param tableId: The table_id of the node/rel.
     * @param offset: The offset of the node/rel.
     */
    public KuzuInternalID(long tableId, long offset) {
        this.tableId = tableId;
        this.offset = offset;
    }
}
