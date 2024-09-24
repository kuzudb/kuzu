package com.kuzudb;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public class ValueTest extends TestBase {

    @Test
    void ValueCreateNull() throws ObjectRefDestroyedException {
        Value value = Value.createNull();
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.ANY);
        value.destroy();
    }

    @Test
    void ValueCreateNullWithDatatype() throws ObjectRefDestroyedException {
        DataType type = new DataType(DataTypeID.INT64, null, 0);
        Value value = Value.createNullWithDataType(type);
        assertFalse(value.isOwnedByCPP());
        type.destroy();

        assertEquals(value.getDataType().getID(), DataTypeID.INT64);
        value.destroy();
    }

    @Test
    void ValueIsNull() throws ObjectRefDestroyedException {
        Value value = new Value(123L);
        assertFalse(value.isOwnedByCPP());
        assertFalse(value.isNull());
        value.destroy();

        value = Value.createNull();
        assertTrue(value.isNull());
        value.destroy();

        DataType type = new DataType(DataTypeID.INT64, null, 0);
        value = Value.createNullWithDataType(type);
        assertTrue(value.isNull());
        type.destroy();
        value.destroy();
    }

    @Test
    void ValueSetNull() throws ObjectRefDestroyedException {
        Value value = new Value(123L);
        assertFalse(value.isOwnedByCPP());
        assertFalse(value.isNull());

        value.setNull(true);
        assertTrue(value.isNull());

        value.setNull(false);
        assertFalse(value.isNull());
        value.destroy();
    }

    @Test
    void ValueCreateDefault() throws ObjectRefDestroyedException {
        DataType type = new DataType(DataTypeID.INT64, null, 0);
        Value value = Value.createDefault(type);
        assertFalse(value.isOwnedByCPP());
        type.destroy();

        assertFalse(value.isNull());
        assertEquals(value.getDataType().getID(), DataTypeID.INT64);
        assertTrue(value.getValue().equals(0L));
        value.destroy();

        type = new DataType(DataTypeID.STRING, null, 0);
        value = Value.createDefault(type);
        assertFalse(value.isOwnedByCPP());
        type.destroy();

        assertFalse(value.isNull());
        assertEquals(value.getDataType().getID(), DataTypeID.STRING);
        assertTrue(value.getValue().equals(""));
        value.destroy();
    }

    @Test
    void ValueCreateBool() throws ObjectRefDestroyedException {
        // bool
        Value value = new Value(true);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.BOOL);
        assertTrue(value.getValue().equals(true));
        value.destroy();

        value = new Value(false);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.BOOL);
        assertTrue(value.getValue().equals(false));
        value.destroy();
    }

    @Test
    void ValueCreateINT16() throws ObjectRefDestroyedException {
        // INT16
        Value value = new Value((short) 123);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.INT16);
        assertTrue(value.getValue().equals((short) 123));
        value.destroy();
    }

    @Test
    void ValueCreateINT32() throws ObjectRefDestroyedException {
        // INT32
        Value value = new Value(123);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.INT32);
        assertTrue(value.getValue().equals(123));
        value.destroy();
    }

    @Test
    void ValueCreateINT64() throws ObjectRefDestroyedException {
        // INT64
        Value value = new Value(123L);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.INT64);
        assertTrue(value.getValue().equals(123L));
        value.destroy();
    }

    @Test
    void ValueCreateFloat() throws ObjectRefDestroyedException {
        // float
        Value value = new Value((float) 123.456);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.FLOAT);
        assertTrue(value.getValue().equals((float) 123.456));
        value.destroy();
    }

    @Test
    void ValueCreateDouble() throws ObjectRefDestroyedException {
        // double
        Value value = new Value((float) 123.456);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.FLOAT);
        assertTrue(value.getValue().equals((float) 123.456));
        value.destroy();
    }

    @Test
    void ValueCreateDecimal() throws ObjectRefDestroyedException {
        // decimal
        Value value = new Value(new BigDecimal("-3.140"));
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.DECIMAL);
        BigDecimal val = (BigDecimal)value.getValue();
        assertTrue(val.compareTo(new BigDecimal("-3.14")) == 0);
        value.destroy();
    }

    @Test
    void ValueCreateInternalID() throws ObjectRefDestroyedException {
        // InternalID
        Value value = new Value(new InternalID(1, 123));
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.INTERNAL_ID);
        InternalID id = value.getValue();
        assertEquals(id.tableId, 1);
        assertEquals(id.offset, 123);
        value.destroy();
    }

    @Test
    void ValueCreateDate() throws ObjectRefDestroyedException {
        // date
        Value value = new Value(LocalDate.ofEpochDay(123));
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.DATE);
        LocalDate date = value.getValue();
        assertEquals(date.toEpochDay(), 123);
        value.destroy();
    }

    @Test
    void ValueCreateTimeStamp() throws ObjectRefDestroyedException {
        // timestamp
        Value value = new Value(Instant.ofEpochSecond(123 / 1000000L, 123 % 1000000 * 1000)); // 123 microseconds
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.TIMESTAMP);
        Instant stamp = value.getValue();
        assertEquals(stamp.getEpochSecond(), 0);
        assertEquals(stamp.getNano(), 123000);
        value.destroy();

        value = new Value(Instant.ofEpochSecond(123123123L / 1000000L, 123123123L % 1000000 * 1000));
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.TIMESTAMP);
        stamp = value.getValue();
        assertEquals(stamp.getEpochSecond(), 123);
        assertEquals(stamp.getNano(), 123123000);
        value.destroy();
    }

    @Test
    void ValueCreateInterval() throws ObjectRefDestroyedException {
        // interval
        Duration inputDuration = Duration.ofMillis(31795200003L);
        Value value = new Value(inputDuration);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.INTERVAL);
        Duration interval = value.getValue();
        assertEquals(interval.toMillis(), inputDuration.toMillis());
        value.destroy();
    }

    @Test
    void ValueCreateString() throws ObjectRefDestroyedException {
        // String
        Value value = new Value("abcdefg");
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.STRING);
        String str = value.getValue();
        assertTrue(str.equals("abcdefg"));
        value.destroy();
    }

    @Test
    void ValueClone() throws ObjectRefDestroyedException {
        Value value = new Value("abcdefg");
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), DataTypeID.STRING);
        String str = value.getValue();
        assertTrue(str.equals("abcdefg"));

        Value clone = value.clone();
        value.destroy();

        assertFalse(clone.isOwnedByCPP());
        assertEquals(clone.getDataType().getID(), DataTypeID.STRING);
        str = clone.getValue();
        assertTrue(str.equals("abcdefg"));
        clone.destroy();
    }

    @Test
    void ValueCopy() throws ObjectRefDestroyedException {
        Value value = new Value("abc");
        Value value2 = new Value("abcdefg");

        value.copy(value2);
        value2.destroy();

        assertFalse(value.isNull());
        assertEquals(value.getDataType().getID(), DataTypeID.STRING);
        String str = value.getValue();
        assertTrue(str.equals("abcdefg"));
        value.destroy();
    }

    @Test
    void ValueGetListSize() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (a:person) RETURN a.workedHours ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);

        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());
        assertEquals(ValueListUtil.getListSize(value), 2);

        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetListElement() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (a:person) RETURN a.workedHours ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());
        assertEquals(ValueListUtil.getListSize(value), 2);

        Value listElement = ValueListUtil.getListElement(value, 0);
        assertTrue(listElement.isOwnedByCPP());
        assertTrue(listElement.getValue().equals(10L));
        listElement.destroy();

        listElement = ValueListUtil.getListElement(value, 1);
        assertTrue(listElement.isOwnedByCPP());
        assertTrue(listElement.getValue().equals(5L));
        listElement.destroy();

        listElement = ValueListUtil.getListElement(value, 222);
        assertNull(listElement);

        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetDatatype() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);

        DataType dataType = value.getDataType();
        assertEquals(dataType.getID(), DataTypeID.STRING);
        dataType.destroy();
        value.destroy();

        value = flatTuple.getValue(1);
        dataType = value.getDataType();
        assertEquals(dataType.getID(), DataTypeID.BOOL);
        dataType.destroy();
        value.destroy();

        value = flatTuple.getValue(2);
        dataType = value.getDataType();
        assertEquals(dataType.getID(), DataTypeID.LIST);
        DataType childDataType = dataType.getChildType();
        assertEquals(childDataType.getID(), DataTypeID.INT64);
        childDataType.destroy();
        dataType.destroy();
        value.destroy();

        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetBool() throws ObjectRefDestroyedException {
        // bool
        QueryResult result = conn.query("MATCH (a:person) RETURN a.isStudent ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(true));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetINT8() throws ObjectRefDestroyedException {
        // INT8
        QueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.level ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((byte) 5));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetINT16() throws ObjectRefDestroyedException {
        // INT16
        QueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.length ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((short) 5));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetINT32() throws ObjectRefDestroyedException {
        // INT32
        QueryResult result = conn.query("MATCH (m:movies) RETURN m.length ORDER BY m.name");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(298));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetINT64() throws ObjectRefDestroyedException {
        // INT64
        QueryResult result = conn.query("MATCH (a:person) RETURN a.ID ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(0L));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetUINT8() throws ObjectRefDestroyedException {
        // UINT8
        QueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulevel ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((short) 250));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetUINT16() throws ObjectRefDestroyedException {
        // UINT16
        QueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulength ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(33768));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetUINT32() throws ObjectRefDestroyedException {
        // UINT32
        QueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.temperature ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(32800L));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetUINT64() throws ObjectRefDestroyedException {
        // UINT64
        QueryResult result = conn.query("RETURN cast(1000043524, \"UINT64\")");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertEquals(value.getValue(), new BigInteger("1000043524"));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetInt128() throws ObjectRefDestroyedException {
        // INT128
        QueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.hugedata ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);

        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertEquals(value.getValue(), new BigInteger("1844674407370955161811111111"));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetSERIAL() throws ObjectRefDestroyedException {
        // SERIAL
        QueryResult result = conn.query("MATCH (a:moviesSerial) WHERE a.ID = 2 RETURN a.ID;");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(2L));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }


    @Test
    void ValueGetFloat() throws ObjectRefDestroyedException {
        // FLOAT
        QueryResult result = conn.query("MATCH (a:person) RETURN a.height ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((float) 1.731));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetDouble() throws ObjectRefDestroyedException {
        // Double
        QueryResult result = conn.query("MATCH (a:person) RETURN a.eyeSight ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((double) 5.0));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetDate() throws ObjectRefDestroyedException {
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
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetTimeStamp() throws ObjectRefDestroyedException {
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
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetTimeStampTz() throws ObjectRefDestroyedException {
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
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetTimeStampNs() throws ObjectRefDestroyedException {
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
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetTimeStampMs() throws ObjectRefDestroyedException {
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
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetTimeStampSec() throws ObjectRefDestroyedException {
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
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetInterval() throws ObjectRefDestroyedException {
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
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetString() throws ObjectRefDestroyedException {
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
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetBlob() throws ObjectRefDestroyedException {
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
    void ValueGetUUID() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("RETURN UUID(\"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11\");");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        UUID uid = value.getValue();
        assertTrue(uid.equals(UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueToString() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        String str = value.toString();
        assertTrue(str.equals("Alice"));
        value.destroy();

        value = flatTuple.getValue(1);
        str = value.toString();
        assertTrue(str.equals("True"));
        value.destroy();

        value = flatTuple.getValue(2);
        str = value.toString();
        assertTrue(str.equals("[10,5]"));
        value.destroy();

        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void NodeValGetID() throws ObjectRefDestroyedException {
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
        value.destroy();
        flatTuple.destroy();
        result.destroy();

    }

    @Test
    void NodeValGetLabelName() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (a:person) RETURN a ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        String label = ValueNodeUtil.getLabelName(value);
        assertEquals(label, "person");
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void NodeValGetProperty() throws ObjectRefDestroyedException {
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
        propertyValue.destroy();
        propertyValue = ValueNodeUtil.getPropertyValueAt(value, 1);
        String propertyValuefName = propertyValue.getValue();
        assertTrue(propertyValuefName.equals("Alice"));
        propertyValue.destroy();
        propertyValue = ValueNodeUtil.getPropertyValueAt(value, 2);
        long propertyValueGender = propertyValue.getValue();
        assertEquals(propertyValueGender, 1);
        propertyValue.destroy();
        propertyValue = ValueNodeUtil.getPropertyValueAt(value, 3);
        boolean propertyValueIsStudent = propertyValue.getValue();
        assertEquals(propertyValueIsStudent, true);
        propertyValue.destroy();

        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void NodeValToString() throws ObjectRefDestroyedException {
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
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }


    @Test
    void RelValGetIDsAndLabel() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (a:person) -[r:knows]-> (b:person) RETURN r ORDER BY a.ID, b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

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

        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void RelValGetProperty() throws ObjectRefDestroyedException {
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

        propertyValue.destroy();
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void RelValToString() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (a:person) -[e:workAt]-> (b:organisation) RETURN e ORDER BY a.ID, b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        String str = ValueRelUtil.toString(value);
        assertEquals(str, "(0:2)-{_LABEL: workAt, _ID: 5:0, year: 2015, grading: [3.800000,2.500000], " +
                "rating: 8.200000}->(1:1)");
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void StructValGetNumFields() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertEquals(ValueStructUtil.getNumFields(value), 14);
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void StructValGetIndexByFieldName() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertEquals(ValueStructUtil.getIndexByFieldName(value, "NOT_EXIST"), -1);

        assertEquals(ValueStructUtil.getIndexByFieldName(value, "rating"), 0);
        assertEquals(ValueStructUtil.getIndexByFieldName(value, "views"), 2);
        assertEquals(ValueStructUtil.getIndexByFieldName(value, "release"), 3);
        assertEquals(ValueStructUtil.getIndexByFieldName(value, "release_ns"), 4);
        assertEquals(ValueStructUtil.getIndexByFieldName(value, "release_ms"), 5);
        assertEquals(ValueStructUtil.getIndexByFieldName(value, "release_sec"), 6);
        assertEquals(ValueStructUtil.getIndexByFieldName(value, "release_tz"), 7);
        assertEquals(ValueStructUtil.getIndexByFieldName(value, "film"), 8);
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void StructValGetFieldNameByIndex() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertNull(ValueStructUtil.getFieldNameByIndex(value, 1024));
        assertNull(ValueStructUtil.getFieldNameByIndex(value, -1));
        assertEquals(ValueStructUtil.getFieldNameByIndex(value, 0), "rating");
        assertEquals(ValueStructUtil.getFieldNameByIndex(value, 2), "views");
        assertEquals(ValueStructUtil.getFieldNameByIndex(value, 3), "release");
        assertEquals(ValueStructUtil.getFieldNameByIndex(value, 4), "release_ns");
        assertEquals(ValueStructUtil.getFieldNameByIndex(value, 5), "release_ms");
        assertEquals(ValueStructUtil.getFieldNameByIndex(value, 6), "release_sec");
        assertEquals(ValueStructUtil.getFieldNameByIndex(value, 7), "release_tz");
        assertEquals(ValueStructUtil.getFieldNameByIndex(value, 8), "film");
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void StructValGetValueByFieldName() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description ORDER BY m.name");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        Value fieldValue = ValueStructUtil.getValueByFieldName(value, "NOT_EXIST");
        assertNull(fieldValue);
        fieldValue = ValueStructUtil.getValueByFieldName(value, "rating");
        assertEquals(fieldValue.getValue(), 1223.0);
        fieldValue.destroy();
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void StructValGetValueByIndex() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description ORDER BY m.name");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        Value fieldValue = ValueStructUtil.getValueByIndex(value, 1024);
        assertNull(fieldValue);
        fieldValue = ValueStructUtil.getValueByIndex(value, -1);
        assertNull(fieldValue);
        fieldValue = ValueStructUtil.getValueByIndex(value, 0);
        assertEquals(fieldValue.getValue(), 1223.0);
        fieldValue.destroy();
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void RecursiveRelGetNodeAndRelList() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (a:person)-[e:studyAt*1..1]->(b:organisation) WHERE a.fName = 'Alice' RETURN e;");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        Value value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());

        Value nodeList = ValueRecursiveRelUtil.getNodeList(value);
        assertTrue(nodeList.isOwnedByCPP());
        assertEquals(ValueListUtil.getListSize(nodeList), 0);
        nodeList.destroy();

        Value relList = ValueRecursiveRelUtil.getRelList(value);
        assertTrue(relList.isOwnedByCPP());
        assertEquals(ValueListUtil.getListSize(relList), 1);

        Value rel = ValueListUtil.getListElement(relList, 0);
        assertTrue(rel.isOwnedByCPP());
        InternalID srcId = ValueRelUtil.getSrcID(rel);
        assertEquals(srcId.tableId, 0);
        assertEquals(srcId.offset, 0);

        InternalID dstId = ValueRelUtil.getDstID(rel);
        assertEquals(dstId.tableId, 1);
        assertEquals(dstId.offset, 0);

        rel.destroy();
        relList.destroy();
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void RdfVariantTest() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (a:T_l) RETURN a.val ORDER BY a.id;");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        int i = 0;
        while (result.hasNext()) {
            FlatTuple flatTuple = result.getNext();
            Value value = flatTuple.getValue(0);
            DataType dataType = ValueRdfVariantUtil.getDataType(value);
            switch (i) {
                case 0: {
                    assertEquals(dataType.getID(), DataTypeID.INT64);
                    assertEquals(12L, (long) ValueRdfVariantUtil.getValue(value));
                    break;
                }
                case 1: {
                    assertEquals(dataType.getID(), DataTypeID.INT32);
                    assertEquals(43, (int) ValueRdfVariantUtil.getValue(value));
                    break;
                }
                case 2: {
                    assertEquals(dataType.getID(), DataTypeID.INT16);
                    assertEquals(33, (short) ValueRdfVariantUtil.getValue(value));
                    break;
                }
                case 3: {
                    assertEquals(dataType.getID(), DataTypeID.INT8);
                    assertEquals(2, (byte) ValueRdfVariantUtil.getValue(value));
                    break;
                }
                case 4: {
                    assertEquals(dataType.getID(), DataTypeID.UINT64);
                    assertEquals(BigInteger.valueOf(90), ValueRdfVariantUtil.getValue(value));
                    break;
                }
                case 5: {
                    assertEquals(dataType.getID(), DataTypeID.UINT32);
                    assertEquals(77L, (long) ValueRdfVariantUtil.getValue(value));
                    break;
                }
                case 6: {
                    assertEquals(dataType.getID(), DataTypeID.UINT16);
                    assertEquals(12, (int) ValueRdfVariantUtil.getValue(value));
                    break;
                }
                case 7: {
                    assertEquals(dataType.getID(), DataTypeID.UINT8);
                    assertEquals(1, (short) ValueRdfVariantUtil.getValue(value));
                    break;
                }
                case 8: {
                    assertEquals(dataType.getID(), DataTypeID.DOUBLE);
                    assertEquals(4.4, (double) ValueRdfVariantUtil.getValue(value), 0.0001);
                    break;
                }
                case 9: {
                    assertEquals(dataType.getID(), DataTypeID.FLOAT);
                    assertEquals(1.2, (float) ValueRdfVariantUtil.getValue(value), 0.0001);
                    break;
                }
                case 10: {
                    assertEquals(dataType.getID(), DataTypeID.BOOL);
                    assertEquals(ValueRdfVariantUtil.getValue(value), true);
                    break;
                }
                case 11: {
                    assertEquals(dataType.getID(), DataTypeID.STRING);
                    assertEquals("hhh", ValueRdfVariantUtil.getValue(value));
                    break;
                }
                case 12: {
                    assertEquals(dataType.getID(), DataTypeID.DATE);
                    LocalDate date = ValueRdfVariantUtil.getValue(value);
                    assertEquals((long) date.toEpochDay(), 19723L);
                    break;
                }
                case 13: {
                    assertEquals(dataType.getID(), DataTypeID.TIMESTAMP);
                    Instant stamp = ValueRdfVariantUtil.getValue(value);
                    assertEquals(stamp.toEpochMilli(), 1704108330000L);
                    break;
                }
                case 14: {
                    assertEquals(dataType.getID(), DataTypeID.INTERVAL);
                    Duration interval = ValueRdfVariantUtil.getValue(value);
                    long month = (long) (interval.toDays() / 30);
                    long day = interval.toDays() - (long) (month * 30);
                    long micros = interval.minusDays(interval.toDays()).toMillis() * 1000;
                    assertEquals(month, 0);
                    assertEquals(day, 2);
                    assertEquals(micros, 0);
                    break;
                }
                case 15: {
                    assertEquals(dataType.getID(), DataTypeID.BLOB);
                    byte[] bytes = ValueRdfVariantUtil.getValue(value);
                    assertEquals(bytes.length, 1);
                    assertEquals(bytes[0], (byte) 0xB2);
                    break;
                }
            }
            value.destroy();
            dataType.destroy();
            flatTuple.destroy();
            i++;
        }
        assertEquals(i, 16);
        result.destroy();
    }
}
