package com.kuzudb;

/**
* Kuzu data type ID.
*/
public enum KuzuDataTypeID {
    ANY(0),
    NODE(10),
    REL(11),
    RECURSIVE_REL(12),
    SERIAL(13),
    BOOL(22),
    INT64(23),
    INT32(24),
    INT16(25),
    INT8(26),
    UINT64(27),
    UINT32(28),
    UINT16(29),
    UINT8(30),
    INT128(31),
    DOUBLE(32),
    FLOAT(33),
    DATE(34),
    TIMESTAMP(35),
    TIMESTAMP_SEC(36),
    TIMESTAMP_MS(37),
    TIMESTAMP_NS(38),
    TIMESTAMP_TZ(39),
    INTERVAL(40),
    INTERNAL_ID(42),
    STRING(50),
    BLOB(51),
    LIST(52),
    ARRAY(53),
    STRUCT(54),
    MAP(55),
    UNION(56),
    UUID(59),;

    public final int value;

    private KuzuDataTypeID(int v) {
        this.value = v;
    }
}
