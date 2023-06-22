package tools.java_api;

public class KuzuInternalID {
    public long table_id;
    public long offset;

    public KuzuInternalID (long table_id, long offset) {
        this.table_id = table_id;
        this.offset = offset;
    }
}
