package com.kuzudb;

/**
 * InternalID type which stores the table_id and offset of a node/rel.
 */
public class InternalID {
    public long tableId;
    public long offset;

    /**
     * Create a InternalID from the given table_id and offset.
     *
     * @param tableId: The table_id of the node/rel.
     * @param offset:  The offset of the node/rel.
     */
    public InternalID(long tableId, long offset) {
        this.tableId = tableId;
        this.offset = offset;
    }
}
