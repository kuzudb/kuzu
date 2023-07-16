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
    DOUBLE(26),
    FLOAT(27),
    DATE(28),
    TIMESTAMP(29),
    INTERVAL(30),
    FIXED_LIST(31),
    INTERNAL_ID(40),
    ARROW_COLUMN(41),
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
