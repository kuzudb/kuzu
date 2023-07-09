package com.kuzudb;

/**
* Kuzu data type ID.
*/
public enum KuzuDataTypeID {
    ANY(0),
    NODE(10),
    REL(11),
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
    STRING(50),
    VAR_LIST(52),
    STRUCT(53);

    public final int value;

    private KuzuDataTypeID(int v) {
        this.value = v;
    }
}
