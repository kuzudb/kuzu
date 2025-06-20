package com.kuzudb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValueTest extends TestBase {

    void checkValueConversion(Value val) {
        Map<String, Value> parameters = Map.of("a", val);
        try (PreparedStatement preparedStatement = conn.prepare("RETURN $a")) {
            assertNotNull(preparedStatement);
            assertTrue(preparedStatement.getErrorMessage().equals(""));
            QueryResult result = conn.execute(preparedStatement, parameters);
            if (result.hasNext()) {
                Value cur = result.getNext().getValue(0);
                assertTrue(val.isNull() && cur.isNull() || val.getValue().equals(cur.getValue()));
            }
        }
    }

    @Test
    void ValueCreateNull() {
        Value value = Value.createNull();
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.ANY);
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateNullWithDatatype() {
        DataType type = new DataType(DataTypeID.INT64, null, 0);
        Value value = Value.createNullWithDataType(type);
        assertFalse(value.isOwnedByCPP());
        type.close();
        assertEquals(value.getDataType().getID(), DataTypeID.INT64);
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueIsNull() {
        Value value = new Value(123L);
        assertFalse(value.isOwnedByCPP());
        assertFalse(value.isNull());
        value.close();

        value = Value.createNull();
        assertTrue(value.isNull());
        value.close();

        DataType type = new DataType(DataTypeID.INT64, null, 0);
        value = Value.createNullWithDataType(type);
        assertTrue(value.isNull());
        type.close();
        value.close();
    }

    @Test
    void ValueSetNull() {
        Value value = new Value(123L);
        assertFalse(value.isOwnedByCPP());
        assertFalse(value.isNull());

        value.setNull(true);
        assertTrue(value.isNull());

        value.setNull(false);
        assertFalse(value.isNull());
        value.close();
    }

    @Test
    void ValueCreateDefault() {
        DataType type = new DataType(DataTypeID.INT64, null, 0);
        Value value = Value.createDefault(type);
        assertFalse(value.isOwnedByCPP());
        type.close();

        assertFalse(value.isNull());
        assertEquals(value.getDataType().getID(), DataTypeID.INT64);
        assertTrue(value.getValue().equals(0L));
        checkValueConversion(value);
        value.close();

        type = new DataType(DataTypeID.STRING, null, 0);
        value = Value.createDefault(type);
        assertFalse(value.isOwnedByCPP());
        type.close();

        assertFalse(value.isNull());
        assertEquals(value.getDataType().getID(), DataTypeID.STRING);
        assertTrue(value.getValue().equals(""));
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateAndCloseDefault() {
        try (DataType type = new DataType(DataTypeID.INT64, null, 0);
                Value value = Value.createDefault(type)) {

            assertFalse(value.isOwnedByCPP());
            assertFalse(value.isNull());
            assertEquals(value.getDataType().getID(), DataTypeID.INT64);
            assertTrue(value.getValue().equals(0L));
        }

        try (DataType type = new DataType(DataTypeID.STRING, null, 0);
                Value value = Value.createDefault(type)) {

            assertFalse(value.isOwnedByCPP());
            assertFalse(value.isNull());
            assertEquals(value.getDataType().getID(), DataTypeID.STRING);
            assertTrue(value.getValue().equals(""));
        }
    }

    @Test
    void ValueCreateBool() {
        // bool
        Value value = new Value(true);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.BOOL);
        assertTrue(value.getValue().equals(true));
        checkValueConversion(value);
        value.close();

        value = new Value(false);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.BOOL);
        assertTrue(value.getValue().equals(false));
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateINT16() {
        // INT16
        Value value = new Value((short) 123);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.INT16);
        assertTrue(value.getValue().equals((short) 123));
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateINT32() {
        // INT32
        Value value = new Value(123);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.INT32);
        assertTrue(value.getValue().equals(123));
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateINT64() {
        // INT64
        Value value = new Value(123L);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.INT64);
        assertTrue(value.getValue().equals(123L));
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateFloat() {
        // float
        Value value = new Value((float) 123.456);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.FLOAT);
        assertTrue(value.getValue().equals((float) 123.456));
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateDouble() {
        // double
        Value value = new Value((float) 123.456);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.FLOAT);
        assertTrue(value.getValue().equals((float) 123.456));
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateDecimal() {
        // decimal
        Value value = new Value(new BigDecimal("-3.140"));
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.DECIMAL);
        BigDecimal val = (BigDecimal) value.getValue();
        assertTrue(val.compareTo(new BigDecimal("-3.14")) == 0);
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateInternalID() {
        // InternalID
        Value value = new Value(new InternalID(1, 123));
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.INTERNAL_ID);
        InternalID id = value.getValue();
        assertEquals(id.tableId, 1);
        assertEquals(id.offset, 123);
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateDate() {
        // date
        Value value = new Value(LocalDate.ofEpochDay(123));
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.DATE);
        LocalDate date = value.getValue();
        assertEquals(date.toEpochDay(), 123);
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateTimeStamp() {
        // timestamp
        Value value = new Value(Instant.ofEpochSecond(123 / 1000000L, 123 % 1000000 * 1000)); // 123 microseconds
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.TIMESTAMP);
        Instant stamp = value.getValue();
        assertEquals(stamp.getEpochSecond(), 0);
        assertEquals(stamp.getNano(), 123000);
        checkValueConversion(value);
        value.close();

        value = new Value(Instant.ofEpochSecond(123123123L / 1000000L, 123123123L % 1000000 * 1000));
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.TIMESTAMP);
        stamp = value.getValue();
        assertEquals(stamp.getEpochSecond(), 123);
        assertEquals(stamp.getNano(), 123123000);
        checkValueConversion(value);
        value.close();

        Instant t = Instant.now();
        Value v = new Value(t);
        Instant vt = v.getValue();
        assertEquals(vt.getEpochSecond(), t.getEpochSecond());
        v.close();
    }

    @Test
    void ValueCreateInterval() {
        // interval
        Duration inputDuration = Duration.ofMillis(31795200003L);
        Value value = new Value(inputDuration);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.INTERVAL);
        Duration interval = value.getValue();
        assertEquals(interval.toMillis(), inputDuration.toMillis());
        checkValueConversion(value);
        value.close();
    }

    @Test
    void ValueCreateString() {
        // String
        Value value = new Value("abcdefg");
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.STRING);
        String str = value.getValue();
        assertTrue(str.equals("abcdefg"));
        checkValueConversion(value);
        value.close();
        Value special = new Value("12🍞x🚗😂😃🧘🏻‍♂️a🌍🍞🚗😂😃🧘🏻‍♂️aSóló🌍 xyz");
        assertTrue(special.toString().equals("12🍞x🚗😂😃🧘🏻‍♂️a🌍🍞🚗😂😃🧘🏻‍♂️aSóló🌍 xyz"));
        checkValueConversion(special);
        special.close();
    }

    @Test
    void ValueClone() {
        Value value = new Value("abcdefg");
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.STRING);
        String str = value.getValue();
        assertTrue(str.equals("abcdefg"));

        Value clone = value.clone();
        value.close();

        assertFalse(clone.isOwnedByCPP());
        assertEquals(clone.getDataType().getID(), DataTypeID.STRING);
        str = clone.getValue();
        assertTrue(str.equals("abcdefg"));
        clone.close();
    }

    @Test
    void ValueCopy() {
        Value value = new Value("abc");
        Value value2 = new Value("abcdefg");

        value.copy(value2);
        value2.close();

        assertFalse(value.isNull());
        assertEquals(value.getDataType().getID(), DataTypeID.STRING);
        String str = value.getValue();
        assertTrue(str.equals("abcdefg"));
        value.close();
    }

    @Test
    void CreateListLiteral() {
        Value[] listValues = { new Value(1), new Value(2), new Value(3) };
        KuzuList list = new KuzuList(listValues);
        Value[] listAsArray = list.toArray();
        assertEquals(listValues.length, list.getListSize());
        for (int i = 0; i < list.getListSize(); ++i) {
            assertEquals((Integer) listValues[i].getValue(), (Integer) list.getListElement(i).getValue());
            assertEquals((Integer) listValues[i].getValue(), (Integer) listAsArray[i].getValue());
        }
        assertNull(list.getListElement(list.getListSize()));

        PreparedStatement stmt = conn.prepare("MATCH (a:person) WHERE a.ID IN $ids RETURN a.ID");
        Map<String, Value> params = Map.of("ids", list.getValue());
        QueryResult result = conn.execute(stmt, params);
        assertTrue(result.isSuccess());

        int[] expectedValues = { 2, 3 };
        assertEquals(expectedValues.length, result.getNumTuples());
        for (int i = 0; i < expectedValues.length; ++i) {
            assertTrue(result.hasNext());
            var nextVal = (Long) result.getNext().getValue(0).getValue();
            assertEquals(Long.valueOf(expectedValues[i]), nextVal);
        }

        assertFalse(result.hasNext());

        result.close();
        list.close();
    }

    @Test
    void CreateListLiteralNested() {
        Value[][] nestedListValues = { { new Value(1), new Value(2), new Value(3) },
                { new Value(4), new Value(5), new Value(6) } };
        KuzuList[] nestedLists = { new KuzuList(nestedListValues[0]), new KuzuList(nestedListValues[1]) };

        Value[] listValues = { nestedLists[0].getValue(), nestedLists[1].getValue() };
        KuzuList list = new KuzuList(listValues);
        assertEquals(listValues.length, list.getListSize());
        for (int i = 0; i < list.getListSize(); ++i) {
            KuzuList nestedList = new KuzuList(list.getListElement(i));
            for (int j = 0; j < nestedListValues[i].length; ++j) {
                assertEquals(nestedListValues[i].length, nestedList.getListSize());
                assertEquals((Integer) nestedListValues[i][j].getValue(),
                        (Integer) nestedList.getListElement(j).getValue());
            }
            assertNull(nestedList.getListElement(nestedListValues[i].length));
            nestedList.close();
        }
        assertNull(list.getListElement(list.getListSize()));

        PreparedStatement stmt = conn.prepare("MATCH (a:person) WHERE a.ID IN list_element($ids, 2) RETURN a.ID");
        Map<String, Value> params = Map.of("ids", list.getValue());
        QueryResult result = conn.execute(stmt, params);
        assertTrue(result.isSuccess());

        int[] expectedValues = { 5 };
        assertEquals(expectedValues.length, result.getNumTuples());
        for (int i = 0; i < expectedValues.length; ++i) {
            assertTrue(result.hasNext());
            var nextVal = (Long) result.getNext().getValue(0).getValue();
            assertEquals(Long.valueOf(expectedValues[i]), nextVal);
        }

        assertFalse(result.hasNext());

        result.close();
        list.close();
    }

    @Test
    void CreateListDefaultValues() {
        int listLength = 5;
        KuzuList list = new KuzuList(new DataType(DataTypeID.INT32), listLength);
        assertEquals(listLength, list.getListSize());
        for (int i = 0; i < listLength; ++i) {
            assertEquals(0, (Integer) list.getListElement(i).getValue());
        }
        assertNull(list.getListElement(list.getListSize()));

        list.close();
    }

    @Test
    void ValueGetListSize() {
        QueryResult result = conn.query("MATCH (a:person) RETURN a.workedHours ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);

        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());
        KuzuList list = new KuzuList(value);
        assertEquals(list.getListSize(), 2);

        value.close();
        flatTuple.close();
        result.close();
        list.close();
    }

    @Test
    void ValueGetListElement() {
        QueryResult result = conn.query("MATCH (a:person) RETURN a.workedHours ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        KuzuList list = new KuzuList(value);
        assertEquals(list.getListSize(), 2);

        Value listElement = list.getListElement(0);
        assertTrue(listElement.isOwnedByCPP());
        assertTrue(listElement.getValue().equals(10L));
        listElement.close();

        listElement = list.getListElement(1);
        assertTrue(listElement.isOwnedByCPP());
        assertTrue(listElement.getValue().equals(5L));
        listElement.close();

        listElement = list.getListElement(222);
        assertNull(listElement);

        value.close();
        flatTuple.close();
        result.close();

        list.close();
    }

    @Test
    void ValueGetDatatype() {
        try (QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours")) {
            assertTrue(result.isSuccess());
            assertTrue(result.hasNext());

            try (FlatTuple flatTuple = result.getNext()) {
                try (Value value = flatTuple.getValue(0)) {
                    DataType dataType = value.getDataType();
                    assertEquals(dataType.getID(), DataTypeID.STRING);
                }

                try (Value value = flatTuple.getValue(1)) {
                    DataType dataType = value.getDataType();
                    assertEquals(dataType.getID(), DataTypeID.BOOL);
                }

                try (Value value = flatTuple.getValue(2)) {
                    DataType dataType = value.getDataType();
                    assertEquals(dataType.getID(), DataTypeID.LIST);
                    try (DataType childDataType = dataType.getChildType()) {
                        assertEquals(childDataType.getID(), DataTypeID.INT64);
                    }
                }
            }
        }
    }

    @Test
    void ValueGetBool() {
        // bool
        QueryResult result = conn.query("MATCH (a:person) RETURN a.isStudent ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(true));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetINT8() {
        // INT8
        QueryResult result = conn
                .query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.level ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((byte) 5));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetINT16() {
        // INT16
        QueryResult result = conn
                .query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.length ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((short) 5));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetINT32() {
        // INT32
        QueryResult result = conn.query("MATCH (m:movies) RETURN m.length ORDER BY m.name");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(298));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetINT64() {
        // INT64
        QueryResult result = conn.query("MATCH (a:person) RETURN a.ID ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(0L));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetUINT8() {
        // UINT8
        QueryResult result = conn
                .query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulevel ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((short) 250));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetUINT16() {
        // UINT16
        QueryResult result = conn
                .query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulength ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(33768));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetUINT32() {
        // UINT32
        QueryResult result = conn
                .query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.temperature ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(32800L));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetUINT64() {
        // UINT64
        QueryResult result = conn.query("RETURN cast(1000043524, \"UINT64\")");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertEquals(value.getValue(), new BigInteger("1000043524"));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetInt128() {
        // INT128
        QueryResult result = conn
                .query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.hugedata ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);

        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertEquals(value.getValue(), new BigInteger("1844674407370955161811111111"));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetSERIAL() {
        // SERIAL
        QueryResult result = conn.query("MATCH (a:moviesSerial) WHERE a.ID = 2 RETURN a.ID;");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(2L));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetFloat() {
        // FLOAT
        QueryResult result = conn.query("MATCH (a:person) RETURN a.height ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((float) 1.731));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetDouble() {
        // Double
        QueryResult result = conn.query("MATCH (a:person) RETURN a.eyeSight ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((double) 5.0));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetDate() {
        // Date
        QueryResult result = conn.query("MATCH (a:person) RETURN a.birthdate ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        LocalDate date = value.getValue();
        assertEquals((long) date.toEpochDay(), -25567L);
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetTimeStamp() {
        // timestamp
        QueryResult result = conn.query("MATCH (a:person) RETURN a.registerTime ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        Instant stamp = value.getValue();
        assertEquals(stamp.toEpochMilli(), 1313839530000L);
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetTimeStampTz() {
        // timestamp_tz
        QueryResult result = conn.query("MATCH (m:movies) RETURN m.description.release_tz");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        Instant stamp = value.getValue();
        assertEquals(stamp.toEpochMilli(), 1313839530123L);
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetTimeStampNs() {
        // timestamp_ns
        QueryResult result = conn.query("MATCH (m:movies) RETURN m.description.release_ns");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        Instant stamp = value.getValue();
        assertEquals(stamp.getNano(), 123456000L);
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetTimeStampMs() {
        // timestamp_ms
        QueryResult result = conn.query("MATCH (m:movies) RETURN m.description.release_ms");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        Instant stamp = value.getValue();
        assertEquals(stamp.toEpochMilli(), 1313839530123L);
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetTimeStampSec() {
        // timestamp_sec
        QueryResult result = conn.query("MATCH (m:movies) RETURN m.description.release_sec");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        Instant stamp = value.getValue();
        assertEquals(stamp.getEpochSecond(), 1313839530L);
        assertEquals(stamp.getNano(), 0L);
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetInterval() {
        // Interval
        QueryResult result = conn.query("MATCH (a:person) RETURN a.lastJobDuration ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        Duration interval = value.getValue();
        long month = (long) (interval.toDays() / 30);
        long day = interval.toDays() - (long) (month * 30);
        long micros = interval.minusDays(interval.toDays()).toMillis() * 1000;
        assertEquals(month, 36);
        assertEquals(day, 2);
        assertEquals(micros, 46920000000L);
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetString() {
        // String
        QueryResult result = conn.query("MATCH (a:person) RETURN a.fName ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        String str = value.getValue();
        assertTrue(str.equals("Alice"));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueGetBlob() {
        QueryResult result = conn.query("RETURN BLOB('\\\\xAA\\\\xBB\\\\xCD\\\\x1A');");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        byte[] bytes = value.getValue();
        assertTrue(bytes.length == 4);
        assertTrue(bytes[0] == (byte) 0xAA);
        assertTrue(bytes[1] == (byte) 0xBB);
        assertTrue(bytes[2] == (byte) 0xCD);
        assertTrue(bytes[3] == (byte) 0x1A);
    }

    @Test
    void ValueGetUUID() {
        QueryResult result = conn.query("RETURN UUID(\"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11\");");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        UUID uid = value.getValue();
        assertTrue(uid.equals(UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")));
        assertTrue(value.clone().getValue().equals(value.getValue()));
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void ValueToString() {
        try (QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours")) {
            assertTrue(result.isSuccess());
            assertTrue(result.hasNext());

            try (FlatTuple flatTuple = result.getNext()) {
                try (Value value = flatTuple.getValue(0)) {
                    String str = value.toString();
                    assertTrue(str.equals("Alice"));
                }

                try (Value value = flatTuple.getValue(1)) {
                    String str = value.toString();
                    assertTrue(str.equals("True"));
                }

                try (Value value = flatTuple.getValue(2)) {
                    String str = value.toString();
                    assertTrue(str.equals("[10,5]"));
                }
            }
        }
    }

    @Test
    void NodeValGetID() {
        QueryResult result = conn.query("MATCH (a:person) RETURN a ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        InternalID id = ValueNodeUtil.getID(value);
        assertEquals(id.tableId, 0);
        assertEquals(id.offset, 0);

        Value cloned = value.clone();
        InternalID id2 = ValueNodeUtil.getID(cloned);
        assertEquals(id2.tableId, 0);
        assertEquals(id2.offset, 0);

        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void NodeValGetLabelName() {
        QueryResult result = conn.query("MATCH (a:person) RETURN a ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        String label = ValueNodeUtil.getLabelName(value);
        assertEquals(label, "person");

        Value cloned = value.clone();
        String label2 = ValueNodeUtil.getLabelName(cloned);
        assertEquals(label2, "person");

        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void NodeValGetProperty() {
        QueryResult result = conn.query("MATCH (a:person) RETURN a ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        String propertyName = ValueNodeUtil.getPropertyNameAt(value, 0);
        assertTrue(propertyName.equals("ID"));
        propertyName = ValueNodeUtil.getPropertyNameAt(value, 1);
        assertTrue(propertyName.equals("fName"));
        propertyName = ValueNodeUtil.getPropertyNameAt(value, 2);
        assertTrue(propertyName.equals("gender"));
        propertyName = ValueNodeUtil.getPropertyNameAt(value, 3);
        assertTrue(propertyName.equals("isStudent"));

        Value propertyValue = ValueNodeUtil.getPropertyValueAt(value, 0);
        long propertyValueID = propertyValue.getValue();
        assertEquals(propertyValueID, 0);
        propertyValue.close();
        propertyValue = ValueNodeUtil.getPropertyValueAt(value, 1);
        String propertyValuefName = propertyValue.getValue();
        assertTrue(propertyValuefName.equals("Alice"));
        propertyValue.close();
        propertyValue = ValueNodeUtil.getPropertyValueAt(value, 2);
        long propertyValueGender = propertyValue.getValue();
        assertEquals(propertyValueGender, 1);
        propertyValue.close();
        propertyValue = ValueNodeUtil.getPropertyValueAt(value, 3);
        boolean propertyValueIsStudent = propertyValue.getValue();
        assertEquals(propertyValueIsStudent, true);
        propertyValue.close();

        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void NodeValToString() {
        QueryResult result = conn.query("MATCH (b:organisation) RETURN b ORDER BY b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        String str = ValueNodeUtil.toString(value);
        assertEquals(str, "{_ID: 1:0, _LABEL: organisation, ID: 1, name: ABFsUni, orgCode: 325, mark: 3.700000, " +
                "score: -2, history: 10 years 5 months 13 hours 24 us, licenseValidInterval: 3 years " +
                "5 days, rating: 1.000000, state: {revenue: 138, location: ['toronto','montr,eal'], " +
                "stock: {price: [96,56], volume: 1000}}, info: 3.120000}");
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void RelValGetIDsAndLabel() {
        QueryResult result = conn.query("MATCH (a:person) -[r:knows]-> (b:person) RETURN r ORDER BY a.ID, b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        InternalID relId = ValueRelUtil.getID(value);
        assertEquals(relId.tableId, 3);
        assertEquals(relId.offset, 0);

        InternalID srcId = ValueRelUtil.getSrcID(value);
        assertEquals(srcId.tableId, 0);
        assertEquals(srcId.offset, 0);

        InternalID dstId = ValueRelUtil.getDstID(value);
        assertEquals(dstId.tableId, 0);
        assertEquals(dstId.offset, 1);

        String label = ValueRelUtil.getLabelName(value);
        assertTrue(label.equals("knows"));

        long size = ValueRelUtil.getPropertySize(value);
        assertEquals(size, 7);

        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void RelValGetProperty() {
        QueryResult result = conn.query("MATCH (a:person) -[e:workAt]-> (b:organisation) RETURN e ORDER BY a.ID, b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());

        String propertyName = ValueRelUtil.getPropertyNameAt(value, 0);
        assertEquals(propertyName, "year");

        propertyName = ValueRelUtil.getPropertyNameAt(value, 1);
        assertEquals(propertyName, "grading");

        propertyName = ValueRelUtil.getPropertyNameAt(value, 2);
        assertEquals(propertyName, "rating");

        Value propertyValue = ValueRelUtil.getPropertyValueAt(value, 0);
        long propertyValueYear = propertyValue.getValue();
        assertEquals(propertyValueYear, 2015);

        propertyValue.close();
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void RelValToString() {
        QueryResult result = conn.query("MATCH (a:person) -[e:workAt]-> (b:organisation) RETURN e ORDER BY a.ID, b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        String str = ValueRelUtil.toString(value);
        assertEquals(str, "(0:2)-{_LABEL: workAt, _ID: 7:0, year: 2015, grading: [3.800000,2.500000], " +
                "rating: 8.200000}->(1:1)");
        value.close();
        flatTuple.close();
        result.close();
    }

    @Test
    void CreateStructLiteral() {
        String[] fieldNames = { "name", "ID", "age" };
        Value[] fieldValues = { new Value("Alice"), new Value(1), new Value(20) };
        KuzuStruct structVal = new KuzuStruct(fieldNames, fieldValues);
        assertEquals(fieldNames.length, structVal.getNumFields());
        for (int i = 0; i < fieldNames.length; ++i) {
            assertEquals(fieldNames[i], structVal.getFieldNameByIndex(i));
            assertEquals(fieldValues[i].toString(), structVal.getValueByFieldName(fieldNames[i]).toString());
            assertEquals(structVal.getValueByIndex(i).toString(),
                    structVal.getValueByFieldName(fieldNames[i]).toString());
        }

        PreparedStatement stmt = conn.prepare("MATCH (p:person) WHERE p.fName = $alice.name RETURN p.age");
        Map<String, Value> params = Map.of("alice", structVal.getValue());
        QueryResult result = conn.execute(stmt, params);

        assertTrue(result.isSuccess());
        assertEquals(1, result.getNumTuples());
        assertEquals(Long.valueOf(35), result.getNext().getValue(0).getValue());

        structVal.close();
    }

    @Test
    void CreateStructLiteralNested() {
        String[] personFieldNames = { "name", "ID" };
        Value[] personFieldValues = { new Value("Alice"), new Value(1) };
        KuzuStruct person = new KuzuStruct(personFieldNames, personFieldValues);

        String[] companyFieldNames = { "name", "boss" };
        Value[] companyFieldValues = { new Value("Company"), person.getValue() };
        KuzuStruct company = new KuzuStruct(companyFieldNames, companyFieldValues);

        KuzuStruct actualPerson = new KuzuStruct(company.getValueByFieldName("boss"));
        assertEquals(personFieldNames.length, actualPerson.getNumFields());
        for (int i = 0; i < personFieldNames.length; ++i) {
            assertEquals(personFieldNames[i], actualPerson.getFieldNameByIndex(i));
            assertEquals(personFieldValues[i].toString(),
                    actualPerson.getValueByFieldName(personFieldNames[i]).toString());
            assertEquals(actualPerson.getValueByIndex(i).toString(),
                    actualPerson.getValueByFieldName(personFieldNames[i]).toString());
        }

        PreparedStatement stmt = conn.prepare("MATCH (p:person) WHERE p.fName = $company.boss.name RETURN p.age");
        Map<String, Value> params = Map.of("company", company.getValue());
        QueryResult result = conn.execute(stmt, params);

        assertTrue(result.isSuccess());
        assertEquals(1, result.getNumTuples());
        assertEquals(Long.valueOf(35), result.getNext().getValue(0).getValue());

        actualPerson.close();
        person.close();
        company.close();
    }

    @Test
    void CreateStructLiteralFromMap() {
        Map<String, Value> fields = Map.of(
                "name", new Value("Alice"),
                "ID", new Value(2),
                "age", new Value(20));
        KuzuStruct structVal = new KuzuStruct(fields);
        assertEquals(fields.size(), structVal.getNumFields());
        for (Map.Entry<String, Value> expectedField : fields.entrySet()) {
            String fieldName = expectedField.getKey();
            Value fieldValue = expectedField.getValue();
            assertEquals(fieldValue.toString(), structVal.getValueByFieldName(fieldName).toString());
            assertEquals(structVal.getValueByIndex(structVal.getIndexByFieldName(fieldName)).toString(),
                    structVal.getValueByFieldName(fieldName).toString());
        }

        Map<String, Value> structAsMap = structVal.toMap();
        assertEquals(structVal.getNumFields(), structAsMap.size());
        for (Map.Entry<String, Value> field : structAsMap.entrySet()) {
            assertEquals(field.getValue().toString(), structVal.getValueByFieldName(field.getKey()).toString());
        }

        PreparedStatement stmt = conn.prepare("MATCH (p:person) WHERE p.ID = $alice.ID RETURN p.fName");
        Map<String, Value> params = Map.of("alice", structVal.getValue());
        QueryResult result = conn.execute(stmt, params);

        assertTrue(result.isSuccess());
        assertEquals(1, result.getNumTuples());
        assertEquals("Bob", result.getNext().getValue(0).getValue());

        structVal.close();
    }

    @Test
    void StructValGetNumFields() {
        QueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        KuzuStruct structVal = new KuzuStruct(value);
        assertEquals(structVal.getNumFields(), 14);
        value.close();
        flatTuple.close();
        result.close();

        structVal.close();
    }

    @Test
    void InternalIDEquality() {
        QueryResult result = conn.query("MATCH (a:person) -[r:knows]-> (b:person) RETURN r ORDER BY a.ID, b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        InternalID relId = ValueRelUtil.getID(value);
        InternalID newInternalID = new InternalID(relId.tableId, relId.offset);
        assertEquals(relId, newInternalID);
        assertTrue(relId.equals(newInternalID));
        assertTrue(relId.equals(relId));
        assertFalse(relId.equals(null));
        assertFalse(relId.equals(new Object()));
    }

    @Test
    void InternalIDAsHashMapKey() {
        HashMap<InternalID, String> map = new HashMap<>();
        InternalID id1 = new InternalID(1, 2);
        InternalID id2 = new InternalID(1, 2);
        InternalID id3 = new InternalID(1, 3);
        map.put(id1, "Alice");
        map.put(id2, "Bob");
        map.put(id3, "Charlie");
        assertEquals(map.size(), 2);
        assertEquals(map.get(id1), "Bob");
        assertEquals(map.get(id3), "Charlie");
    }

    @Test
    void StructValGetIndexByFieldName() {
        QueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        KuzuStruct structVal = new KuzuStruct(value);
        assertEquals(structVal.getIndexByFieldName("NOT_EXIST"), -1);

        assertEquals(structVal.getIndexByFieldName("rating"), 0);
        assertEquals(structVal.getIndexByFieldName("views"), 2);
        assertEquals(structVal.getIndexByFieldName("release"), 3);
        assertEquals(structVal.getIndexByFieldName("release_ns"), 4);
        assertEquals(structVal.getIndexByFieldName("release_ms"), 5);
        assertEquals(structVal.getIndexByFieldName("release_sec"), 6);
        assertEquals(structVal.getIndexByFieldName("release_tz"), 7);
        assertEquals(structVal.getIndexByFieldName("film"), 8);

        value.close();
        flatTuple.close();
        result.close();
        structVal.close();
    }

    @Test
    void StructValGetFieldNameByIndex() {
        QueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        KuzuStruct structVal = new KuzuStruct(value);
        assertNull(structVal.getFieldNameByIndex(1024));
        assertNull(structVal.getFieldNameByIndex(-1));
        assertEquals(structVal.getFieldNameByIndex(0), "rating");
        assertEquals(structVal.getFieldNameByIndex(2), "views");
        assertEquals(structVal.getFieldNameByIndex(3), "release");
        assertEquals(structVal.getFieldNameByIndex(4), "release_ns");
        assertEquals(structVal.getFieldNameByIndex(5), "release_ms");
        assertEquals(structVal.getFieldNameByIndex(6), "release_sec");
        assertEquals(structVal.getFieldNameByIndex(7), "release_tz");
        assertEquals(structVal.getFieldNameByIndex(8), "film");

        value.close();
        flatTuple.close();
        result.close();
        structVal.close();
    }

    @Test
    void StructValGetValueByFieldName() {
        QueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description ORDER BY m.name");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        KuzuStruct structVal = new KuzuStruct(value);
        Value fieldValue = structVal.getValueByFieldName("NOT_EXIST");
        assertNull(fieldValue);
        fieldValue = structVal.getValueByFieldName("rating");
        assertEquals(fieldValue.getValue(), 1223.0);

        fieldValue.close();
        value.close();
        flatTuple.close();
        result.close();
        structVal.close();
    }

    @Test
    void StructValGetValueByIndex() {
        QueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description ORDER BY m.name");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        KuzuStruct structVal = new KuzuStruct(value);
        Value fieldValue = structVal.getValueByIndex(1024);
        assertNull(fieldValue);
        fieldValue = structVal.getValueByIndex(-1);
        assertNull(fieldValue);
        fieldValue = structVal.getValueByIndex(0);
        assertEquals(fieldValue.getValue(), 1223.0);

        fieldValue.close();
        value.close();
        flatTuple.close();
        result.close();
        structVal.close();
    }

    @Test
    void CreateMapLiteral() {
        Value[] keys = { new Value("Alice"), new Value("Bob") };
        Value[] values = { new Value(1), new Value(2) };
        KuzuMap kuzuMap = new KuzuMap(keys, values);
        assertEquals(keys.length, kuzuMap.getNumFields());
        int aliceKeyIdx = (kuzuMap.getKey(0).getValue().equals("Alice")) ? 0 : 1;
        int bobKeyIdx = 1 - aliceKeyIdx;
        assertEquals("Alice", kuzuMap.getKey(aliceKeyIdx).getValue());
        assertEquals(1, (Integer) kuzuMap.getValue(aliceKeyIdx).getValue());
        assertEquals("Bob", kuzuMap.getKey(bobKeyIdx).getValue());
        assertEquals(2, (Integer) kuzuMap.getValue(bobKeyIdx).getValue());

        PreparedStatement stmt = conn
                .prepare("MATCH (m:movies) WHERE m.name = 'Roma' SET m.audience = $audience");
        Map<String, Value> options = Map.of(
                "audience", kuzuMap.getValue());
        QueryResult result = conn.execute(stmt, options);

        assertTrue(result.isSuccess());
        kuzuMap.close();
    }

    @Test
    void CreateMapLiteralNested() {
        Value[][] nestedKeys = { { new Value("Alice"), new Value("Bob") }, { new Value("Carol"), new Value("Dan") } };
        Value[][] nestedValues = { { new Value(1), new Value(2) }, { new Value(3), new Value(4) } };
        KuzuMap map0 = new KuzuMap(nestedKeys[0], nestedValues[0]);
        KuzuMap map1 = new KuzuMap(nestedKeys[1], nestedValues[1]);
        Value[] nestedMaps = { map0.getValue(), map1.getValue() };

        Value[] keys = { new Value(Long.valueOf(0)), new Value(Long.valueOf(1)) };
        KuzuMap kuzuMap = new KuzuMap(keys, nestedMaps);
        assertEquals(keys.length, kuzuMap.getNumFields());

        KuzuMap map1Actual = new KuzuMap(kuzuMap.getValue(1));
        int carolKeyIdx = (map1Actual.getKey(0).getValue().equals("Carol")) ? 0 : 1;
        int danKeyIdx = 1 - carolKeyIdx;
        assertEquals("Carol", map1Actual.getKey(carolKeyIdx).getValue());
        assertEquals(3, (Integer) map1Actual.getValue(carolKeyIdx).getValue());
        assertEquals("Dan", map1Actual.getKey(danKeyIdx).getValue());
        assertEquals(4, (Integer) map1Actual.getValue(danKeyIdx).getValue());

        conn.query("BEGIN TRANSACTION");

        PreparedStatement stmt = conn
                .prepare(
                        "MATCH (m:movies) WHERE m.name = 'Roma' SET m.audience = list_element(element_at($audience, CAST(1, 'INT64')), 1)");
        Map<String, Value> options = Map.of(
                "audience", kuzuMap.getValue());
        QueryResult result = conn.execute(stmt, options);
        assertTrue(result.isSuccess());

        result = conn
                .query("MATCH (m:movies) WHERE m.name = 'Roma' RETURN list_element(element_at(m.audience, 'Dan'), 1)");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getNumTuples());
        assertEquals(4, (Long) result.getNext().getValue(0).getValue());

        kuzuMap.close();
        map1Actual.close();
        map0.close();
        map1.close();

        // rollback the changes so it doesn't affect other tests
        conn.query("ROLLBACK");
    }

    @Test
    void MapValGetNumFields() {
        QueryResult result = conn.query("MATCH (m:movies) RETURN m.audience ORDER BY m.length");
        assertTrue(result.isSuccess());

        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        KuzuMap mapVal = new KuzuMap(value);
        assertEquals(mapVal.getNumFields(), 2);
        value.close();
        flatTuple.close();
        mapVal.close();

        flatTuple = result.getNext();
        value = flatTuple.getValue(0);
        mapVal = new KuzuMap(value);
        assertTrue(value.isOwnedByCPP());
        assertEquals(mapVal.getNumFields(), 0);
        value.close();
        flatTuple.close();
        mapVal.close();

        flatTuple = result.getNext();
        value = flatTuple.getValue(0);
        mapVal = new KuzuMap(value);
        assertTrue(value.isOwnedByCPP());
        assertEquals(mapVal.getNumFields(), 1);
        value.close();
        flatTuple.close();
        mapVal.close();

        assertFalse(result.hasNext());
        result.close();
    }

    @Test
    void MapValGetKey() {
        QueryResult result = conn.query("MATCH (m:movies) RETURN m.audience ORDER BY m.length");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        KuzuMap mapVal = new KuzuMap(value);
        Value key = mapVal.getKey(0);
        String fieldName = key.getValue();
        assertEquals(fieldName, "audience1");
        key = mapVal.getKey(1);
        fieldName = key.getValue();
        assertEquals(fieldName, "audience53");
        key = mapVal.getKey(-1);
        assertNull(key);
        key = mapVal.getKey(1024);
        assertNull(key);
        value.close();
        flatTuple.close();

        flatTuple = result.getNext();
        value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        key = mapVal.getKey(0);
        assertNull(key);
        value.close();
        flatTuple.close();

        flatTuple = result.getNext();
        value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        key = mapVal.getKey(0);
        fieldName = key.getValue();
        assertEquals(fieldName, "audience1");
        value.close();
        flatTuple.close();

        assertFalse(result.hasNext());
        result.close();
        mapVal.close();
    }

    @Test
    void MapValGetValue() {
        QueryResult result = conn.query("MATCH (m:movies) RETURN m.audience ORDER BY m.length");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        KuzuMap mapVal = new KuzuMap(value);
        Value fieldValue = mapVal.getValue(1024);
        assertNull(fieldValue);
        fieldValue = mapVal.getValue(-1);
        assertNull(fieldValue);
        fieldValue = mapVal.getValue(0);
        assertEquals((long) fieldValue.getValue(), 52);
        fieldValue.close();
        fieldValue = mapVal.getValue(1);
        assertEquals((long) fieldValue.getValue(), 42);
        value.close();
        flatTuple.close();
        mapVal.close();

        flatTuple = result.getNext();
        value = flatTuple.getValue(0);
        mapVal = new KuzuMap(value);
        assertTrue(value.isOwnedByCPP());
        fieldValue = mapVal.getValue(0);
        assertNull(fieldValue);
        value.close();
        flatTuple.close();
        mapVal.close();

        flatTuple = result.getNext();
        value = flatTuple.getValue(0);
        mapVal = new KuzuMap(value);
        assertTrue(value.isOwnedByCPP());
        fieldValue = mapVal.getValue(0);
        assertEquals((long) fieldValue.getValue(), 33);
        fieldValue.close();
        value.close();
        flatTuple.close();
        mapVal.close();

        assertFalse(result.hasNext());
        result.close();
    }

    @Test
    void RecursiveRelGetNodeAndRelList() {
        try (QueryResult result = conn
                .query("MATCH (a:person)-[e:studyAt*1..1]->(b:organisation) WHERE a.fName = 'Alice' RETURN e;")) {
            assertTrue(result.isSuccess());
            assertTrue(result.hasNext());

            try (FlatTuple flatTuple = result.getNext();
                    Value value = flatTuple.getValue(0)) {

                assertTrue(value.isOwnedByCPP());

                try (Value nodeListValue = ValueRecursiveRelUtil.getNodeList(value)) {
                    assertTrue(nodeListValue.isOwnedByCPP());
                    KuzuList nodeList = new KuzuList(nodeListValue);
                    assertEquals(nodeList.getListSize(), 0);
                    nodeList.close();
                }

                try (Value relListValue = ValueRecursiveRelUtil.getRelList(value)) {
                    assertTrue(relListValue.isOwnedByCPP());
                    KuzuList relList = new KuzuList(relListValue);
                    assertEquals(relList.getListSize(), 1);

                    try (Value rel = relList.getListElement(0)) {
                        assertTrue(rel.isOwnedByCPP());

                        InternalID srcId = ValueRelUtil.getSrcID(rel);
                        assertEquals(srcId.tableId, 0);
                        assertEquals(srcId.offset, 0);

                        InternalID dstId = ValueRelUtil.getDstID(rel);
                        assertEquals(dstId.tableId, 1);
                        assertEquals(dstId.offset, 0);
                    }

                    relList.close();
                }
            }
        }
    }
}
