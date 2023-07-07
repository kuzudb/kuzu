package com.kuzudb;

public class KuzuInternalID {
    public long tableId;
    public long offset;

    public KuzuInternalID(long tableId, long offset) {
        this.tableId = tableId;
        this.offset = offset;
    }
}
