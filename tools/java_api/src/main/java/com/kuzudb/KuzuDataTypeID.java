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
    INTERVAL(36),
    FIXED_LIST(37),
    INTERNAL_ID(40),
    STRING(50),
    BLOB(51),
    VAR_LIST(52),
    STRUCT(53),
    MAP(54),
    UNION(55);

    public final int value;

    private KuzuDataTypeID(int v) {
        this.value = v;
    }
}
