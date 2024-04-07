package com.kuzudb.java_test;


import com.kuzudb.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public class ValueTest extends TestBase {

    @Test
    void ValueCreateNull() throws KuzuObjectRefDestroyedException {
        KuzuValue value = KuzuValue.createNull();
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.ANY);
        value.destroy();
    }

    @Test
    void ValueCreateNullWithDatatype() throws KuzuObjectRefDestroyedException {
        KuzuDataType type = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        KuzuValue value = KuzuValue.createNullWithDataType(type);
        assertFalse(value.isOwnedByCPP());
        type.destroy();

        assertEquals(value.getDataType().getID(), KuzuDataTypeID.INT64);
        value.destroy();
    }

    @Test
    void ValueIsNull() throws KuzuObjectRefDestroyedException {
        KuzuValue value = new KuzuValue(123L);
        assertFalse(value.isOwnedByCPP());
        assertFalse(value.isNull());
        value.destroy();

        value = KuzuValue.createNull();
        assertTrue(value.isNull());
        value.destroy();

        KuzuDataType type = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        value = KuzuValue.createNullWithDataType(type);
        assertTrue(value.isNull());
        type.destroy();
        value.destroy();
    }

    @Test
    void ValueSetNull() throws KuzuObjectRefDestroyedException {
        KuzuValue value = new KuzuValue(123L);
        assertFalse(value.isOwnedByCPP());
        assertFalse(value.isNull());

        value.setNull(true);
        assertTrue(value.isNull());

        value.setNull(false);
        assertFalse(value.isNull());
        value.destroy();
    }

    @Test
    void ValueCreateDefault() throws KuzuObjectRefDestroyedException {
        KuzuDataType type = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        KuzuValue value = KuzuValue.createDefault(type);
        assertFalse(value.isOwnedByCPP());
        type.destroy();

        assertFalse(value.isNull());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.INT64);
        assertTrue(value.getValue().equals(0L));
        value.destroy();

        type = new KuzuDataType(KuzuDataTypeID.STRING, null, 0);
        value = KuzuValue.createDefault(type);
        assertFalse(value.isOwnedByCPP());
        type.destroy();

        assertFalse(value.isNull());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.STRING);
        assertTrue(value.getValue().equals(""));
        value.destroy();
    }

    @Test
    void ValueCreateBool() throws KuzuObjectRefDestroyedException {
        // bool
        KuzuValue value = new KuzuValue(true);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.BOOL);
        assertTrue(value.getValue().equals(true));
        value.destroy();

        value = new KuzuValue(false);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.BOOL);
        assertTrue(value.getValue().equals(false));
        value.destroy();
    }

    @Test
    void ValueCreateINT16() throws KuzuObjectRefDestroyedException {
        // INT16
        KuzuValue value = new KuzuValue((short) 123);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.INT16);
        assertTrue(value.getValue().equals((short) 123));
        value.destroy();
    }

    @Test
    void ValueCreateINT32() throws KuzuObjectRefDestroyedException {
        // INT32
        KuzuValue value = new KuzuValue(123);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.INT32);
        assertTrue(value.getValue().equals(123));
        value.destroy();
    }

    @Test
    void ValueCreateINT64() throws KuzuObjectRefDestroyedException {
        // INT64
        KuzuValue value = new KuzuValue(123L);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.INT64);
        assertTrue(value.getValue().equals(123L));
        value.destroy();
    }

    @Test
    void ValueCreateFloat() throws KuzuObjectRefDestroyedException {
        // float
        KuzuValue value = new KuzuValue((float) 123.456);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.FLOAT);
        assertTrue(value.getValue().equals((float) 123.456));
        value.destroy();
    }

    @Test
    void ValueCreateDouble() throws KuzuObjectRefDestroyedException {
        // double
        KuzuValue value = new KuzuValue((float) 123.456);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.FLOAT);
        assertTrue(value.getValue().equals((float) 123.456));
        value.destroy();
    }

    @Test
    void ValueCreateInternalID() throws KuzuObjectRefDestroyedException {
        // InternalID
        KuzuValue value = new KuzuValue(new KuzuInternalID(1, 123));
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.INTERNAL_ID);
        KuzuInternalID id = value.getValue();
        assertEquals(id.tableId, 1);
        assertEquals(id.offset, 123);
        value.destroy();
    }

    @Test
    void ValueCreateDate() throws KuzuObjectRefDestroyedException {
        // date
        KuzuValue value = new KuzuValue(LocalDate.ofEpochDay(123));
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.DATE);
        LocalDate date = value.getValue();
        assertEquals(date.toEpochDay(), 123);
        value.destroy();
    }

    @Test
    void ValueCreateTimeStamp() throws KuzuObjectRefDestroyedException {
        // timestamp
        KuzuValue value = new KuzuValue(Instant.ofEpochSecond(123 / 1000000L, 123 % 1000000 * 1000)); // 123 microseconds
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.TIMESTAMP);
        Instant stamp = value.getValue();
        assertEquals(stamp.getEpochSecond(), 0);
        assertEquals(stamp.getNano(), 123000);
        value.destroy();

        value = new KuzuValue(Instant.ofEpochSecond(123123123L / 1000000L, 123123123L % 1000000 * 1000));
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.TIMESTAMP);
        stamp = value.getValue();
        assertEquals(stamp.getEpochSecond(), 123);
        assertEquals(stamp.getNano(), 123123000);
        value.destroy();
    }

    @Test
    void ValueCreateInterval() throws KuzuObjectRefDestroyedException {
        // interval
        Duration inputDuration = Duration.ofMillis(31795200003L);
        KuzuValue value = new KuzuValue(inputDuration);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.INTERVAL);
        Duration interval = value.getValue();
        assertEquals(interval.toMillis(), inputDuration.toMillis());
        value.destroy();
    }

    @Test
    void ValueCreateString() throws KuzuObjectRefDestroyedException {
        // String
        KuzuValue value = new KuzuValue("abcdefg");
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.STRING);
        String str = value.getValue();
        assertTrue(str.equals("abcdefg"));
        value.destroy();
    }

    @Test
    void ValueClone() throws KuzuObjectRefDestroyedException {
        KuzuValue value = new KuzuValue("abcdefg");
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.STRING);
        String str = value.getValue();
        assertTrue(str.equals("abcdefg"));

        KuzuValue clone = value.clone();
        value.destroy();

        assertFalse(clone.isOwnedByCPP());
        assertEquals(clone.getDataType().getID(), KuzuDataTypeID.STRING);
        str = clone.getValue();
        assertTrue(str.equals("abcdefg"));
        clone.destroy();
    }

    @Test
    void ValueCopy() throws KuzuObjectRefDestroyedException {
        KuzuValue value = new KuzuValue("abc");
        KuzuValue value2 = new KuzuValue("abcdefg");

        value.copy(value2);
        value2.destroy();

        assertFalse(value.isNull());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.STRING);
        String str = value.getValue();
        assertTrue(str.equals("abcdefg"));
        value.destroy();
    }

    @Test
    void ValueGetListSize() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.workedHours ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);

        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());
        assertEquals(KuzuValueListUtil.getListSize(value), 2);

        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetListElement() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.workedHours ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());
        assertEquals(KuzuValueListUtil.getListSize(value), 2);

        KuzuValue listElement = KuzuValueListUtil.getListElement(value, 0);
        assertTrue(listElement.isOwnedByCPP());
        assertTrue(listElement.getValue().equals(10L));
        listElement.destroy();

        listElement = KuzuValueListUtil.getListElement(value, 1);
        assertTrue(listElement.isOwnedByCPP());
        assertTrue(listElement.getValue().equals(5L));
        listElement.destroy();

        listElement = KuzuValueListUtil.getListElement(value, 222);
        assertNull(listElement);

        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetDatatype() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);

        KuzuDataType dataType = value.getDataType();
        assertEquals(dataType.getID(), KuzuDataTypeID.STRING);
        dataType.destroy();
        value.destroy();

        value = flatTuple.getValue(1);
        dataType = value.getDataType();
        assertEquals(dataType.getID(), KuzuDataTypeID.BOOL);
        dataType.destroy();
        value.destroy();

        value = flatTuple.getValue(2);
        dataType = value.getDataType();
        assertEquals(dataType.getID(), KuzuDataTypeID.LIST);
        KuzuDataType childDataType = dataType.getChildType();
        assertEquals(childDataType.getID(), KuzuDataTypeID.INT64);
        childDataType.destroy();
        dataType.destroy();
        value.destroy();

        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetBool() throws KuzuObjectRefDestroyedException {
        // bool
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.isStudent ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(true));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetINT8() throws KuzuObjectRefDestroyedException {
        // INT8
        KuzuQueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.level ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((byte) 5));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetINT16() throws KuzuObjectRefDestroyedException {
        // INT16
        KuzuQueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.length ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((short) 5));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetINT32() throws KuzuObjectRefDestroyedException {
        // INT32
        KuzuQueryResult result = conn.query("MATCH (m:movies) RETURN m.length ORDER BY m.name");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(298));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetINT64() throws KuzuObjectRefDestroyedException {
        // INT64
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.ID ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(0L));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetUINT8() throws KuzuObjectRefDestroyedException {
        // UINT8
        KuzuQueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulevel ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((short) 250));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetUINT16() throws KuzuObjectRefDestroyedException {
        // UINT16
        KuzuQueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulength ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(33768));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetUINT32() throws KuzuObjectRefDestroyedException {
        // UINT32
        KuzuQueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.temprature ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(32800L));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetUINT64() throws KuzuObjectRefDestroyedException {
        // UINT64
        KuzuQueryResult result = conn.query("RETURN cast(1000043524, \"UINT64\")");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertEquals(value.getValue(), new BigInteger("1000043524"));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetInt128() throws KuzuObjectRefDestroyedException {
        // INT128
        KuzuQueryResult result = conn.query("MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.hugedata ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);

        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertEquals(value.getValue(), new BigInteger("1844674407370955161811111111"));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetSERIAL() throws KuzuObjectRefDestroyedException {
        // SERIAL
        KuzuQueryResult result = conn.query("MATCH (a:moviesSerial) WHERE a.ID = 2 RETURN a.ID;");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals(2L));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }


    @Test
    void ValueGetFloat() throws KuzuObjectRefDestroyedException {
        // FLOAT
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.height ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((float) 1.731));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetDouble() throws KuzuObjectRefDestroyedException {
        // Double
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.eyeSight ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        assertTrue(value.getValue().equals((double) 5.0));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetDate() throws KuzuObjectRefDestroyedException {
        // Date
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.birthdate ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        LocalDate date = value.getValue();
        assertEquals((long) date.toEpochDay(), -25567L);
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetTimeStamp() throws KuzuObjectRefDestroyedException {
        // timestamp
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.registerTime ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        Instant stamp = value.getValue();
        assertEquals(stamp.toEpochMilli(), 1313839530000L);
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetTimeStampTz() throws KuzuObjectRefDestroyedException {
        // timestamp_tz
        KuzuQueryResult result = conn.query("MATCH (m:movies) RETURN m.description.release_tz");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        Instant stamp = value.getValue();
        assertEquals(stamp.toEpochMilli(), 1313839530123L);
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetTimeStampNs() throws KuzuObjectRefDestroyedException {
        // timestamp_ns
        KuzuQueryResult result = conn.query("MATCH (m:movies) RETURN m.description.release_ns");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        Instant stamp = value.getValue();
        assertEquals(stamp.getNano(), 123456000L);
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetTimeStampMs() throws KuzuObjectRefDestroyedException {
        // timestamp_ms
        KuzuQueryResult result = conn.query("MATCH (m:movies) RETURN m.description.release_ms");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        Instant stamp = value.getValue();
        assertEquals(stamp.toEpochMilli(), 1313839530123L);
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetTimeStampSec() throws KuzuObjectRefDestroyedException {
        // timestamp_sec
        KuzuQueryResult result = conn.query("MATCH (m:movies) RETURN m.description.release_sec");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
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
    void ValueGetInterval() throws KuzuObjectRefDestroyedException {
        // Interval
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.lastJobDuration ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
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
    void ValueGetString() throws KuzuObjectRefDestroyedException {
        // String
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        String str = value.getValue();
        assertTrue(str.equals("Alice"));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetBlob() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("RETURN BLOB('\\\\xAA\\\\xBB\\\\xCD\\\\x1A');");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
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
    void ValueGetUUID() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("RETURN UUID(\"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11\");");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        UUID uid = value.getValue();
        assertTrue(uid.equals(UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")));
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueToString() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
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
    void NodeValGetID() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        KuzuInternalID id = KuzuValueNodeUtil.getID(value);
        assertEquals(id.tableId, 0);
        assertEquals(id.offset, 0);
        value.destroy();
        flatTuple.destroy();
        result.destroy();

    }

    @Test
    void NodeValGetLabelName() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        String label = KuzuValueNodeUtil.getLabelName(value);
        assertEquals(label, "person");
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void NodeValGetProperty() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        String propertyName = KuzuValueNodeUtil.getPropertyNameAt(value, 0);
        assertTrue(propertyName.equals("ID"));
        propertyName = KuzuValueNodeUtil.getPropertyNameAt(value, 1);
        assertTrue(propertyName.equals("fName"));
        propertyName = KuzuValueNodeUtil.getPropertyNameAt(value, 2);
        assertTrue(propertyName.equals("gender"));
        propertyName = KuzuValueNodeUtil.getPropertyNameAt(value, 3);
        assertTrue(propertyName.equals("isStudent"));

        KuzuValue propertyValue = KuzuValueNodeUtil.getPropertyValueAt(value, 0);
        long propertyValueID = propertyValue.getValue();
        assertEquals(propertyValueID, 0);
        propertyValue.destroy();
        propertyValue = KuzuValueNodeUtil.getPropertyValueAt(value, 1);
        String propertyValuefName = propertyValue.getValue();
        assertTrue(propertyValuefName.equals("Alice"));
        propertyValue.destroy();
        propertyValue = KuzuValueNodeUtil.getPropertyValueAt(value, 2);
        long propertyValueGender = propertyValue.getValue();
        assertEquals(propertyValueGender, 1);
        propertyValue.destroy();
        propertyValue = KuzuValueNodeUtil.getPropertyValueAt(value, 3);
        boolean propertyValueIsStudent = propertyValue.getValue();
        assertEquals(propertyValueIsStudent, true);
        propertyValue.destroy();

        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void NodeValToString() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (b:organisation) RETURN b ORDER BY b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        String str = KuzuValueNodeUtil.toString(value);
        assertEquals(str, "{_ID: 1:0, _LABEL: organisation, ID: 1, name: ABFsUni, orgCode: 325, mark: 3.700000, " +
                "score: -2, history: 10 years 5 months 13 hours 24 us, licenseValidInterval: 3 years " +
                "5 days, rating: 1.000000, state: {revenue: 138, location: ['toronto','montr,eal'], " +
                "stock: {price: [96,56], volume: 1000}}, info: 3.120000}");
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }


    @Test
    void RelValGetIDsAndLabel() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) -[r:knows]-> (b:person) RETURN r ORDER BY a.ID, b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        KuzuInternalID srcId = KuzuValueRelUtil.getSrcID(value);
        assertEquals(srcId.tableId, 0);
        assertEquals(srcId.offset, 0);

        KuzuInternalID dstId = KuzuValueRelUtil.getDstID(value);
        assertEquals(dstId.tableId, 0);
        assertEquals(dstId.offset, 1);

        String label = KuzuValueRelUtil.getLabelName(value);
        assertTrue(label.equals("knows"));

        long size = KuzuValueRelUtil.getPropertySize(value);
        assertEquals(size, 7);

        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void RelValGetProperty() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) -[e:workAt]-> (b:organisation) RETURN e ORDER BY a.ID, b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());

        String propertyName = KuzuValueRelUtil.getPropertyNameAt(value, 0);
        assertEquals(propertyName, "year");

        propertyName = KuzuValueRelUtil.getPropertyNameAt(value, 1);
        assertEquals(propertyName, "grading");

        propertyName = KuzuValueRelUtil.getPropertyNameAt(value, 2);
        assertEquals(propertyName, "rating");

        KuzuValue propertyValue = KuzuValueRelUtil.getPropertyValueAt(value, 0);
        long propertyValueYear = propertyValue.getValue();
        assertEquals(propertyValueYear, 2015);

        propertyValue.destroy();
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void RelValToString() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) -[e:workAt]-> (b:organisation) RETURN e ORDER BY a.ID, b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        String str = KuzuValueRelUtil.toString(value);
        assertEquals(str, "(0:2)-{_LABEL: workAt, _ID: 5:0, year: 2015, grading: [3.800000,2.500000], " +
                "rating: 8.200000}->(1:1)");
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void StructValGetNumFields() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertEquals(KuzuValueStructUtil.getNumFields(value), 14);
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void StructValGetIndexByFieldName() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertEquals(KuzuValueStructUtil.getIndexByFieldName(value, "NOT_EXIST"), -1);

        assertEquals(KuzuValueStructUtil.getIndexByFieldName(value, "rating"), 0);
        assertEquals(KuzuValueStructUtil.getIndexByFieldName(value, "views"), 2);
        assertEquals(KuzuValueStructUtil.getIndexByFieldName(value, "release"), 3);
        assertEquals(KuzuValueStructUtil.getIndexByFieldName(value, "release_ns"), 4);
        assertEquals(KuzuValueStructUtil.getIndexByFieldName(value, "release_ms"), 5);
        assertEquals(KuzuValueStructUtil.getIndexByFieldName(value, "release_sec"), 6);
        assertEquals(KuzuValueStructUtil.getIndexByFieldName(value, "release_tz"), 7);
        assertEquals(KuzuValueStructUtil.getIndexByFieldName(value, "film"), 8);
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void StructValGetFieldNameByIndex() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertNull(KuzuValueStructUtil.getFieldNameByIndex(value, 1024));
        assertNull(KuzuValueStructUtil.getFieldNameByIndex(value, -1));
        assertEquals(KuzuValueStructUtil.getFieldNameByIndex(value, 0), "rating");
        assertEquals(KuzuValueStructUtil.getFieldNameByIndex(value, 2), "views");
        assertEquals(KuzuValueStructUtil.getFieldNameByIndex(value, 3), "release");
        assertEquals(KuzuValueStructUtil.getFieldNameByIndex(value, 4), "release_ns");
        assertEquals(KuzuValueStructUtil.getFieldNameByIndex(value, 5), "release_ms");
        assertEquals(KuzuValueStructUtil.getFieldNameByIndex(value, 6), "release_sec");
        assertEquals(KuzuValueStructUtil.getFieldNameByIndex(value, 7), "release_tz");
        assertEquals(KuzuValueStructUtil.getFieldNameByIndex(value, 8), "film");
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void StructValGetValueByFieldName() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description ORDER BY m.name");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        KuzuValue fieldValue = KuzuValueStructUtil.getValueByFieldName(value, "NOT_EXIST");
        assertNull(fieldValue);
        fieldValue = KuzuValueStructUtil.getValueByFieldName(value, "rating");
        assertEquals(fieldValue.getValue(), 1223.0);
        fieldValue.destroy();
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void StructValGetValueByIndex() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description ORDER BY m.name");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        KuzuValue fieldValue = KuzuValueStructUtil.getValueByIndex(value, 1024);
        assertNull(fieldValue);
        fieldValue = KuzuValueStructUtil.getValueByIndex(value, -1);
        assertNull(fieldValue);
        fieldValue = KuzuValueStructUtil.getValueByIndex(value, 0);
        assertEquals(fieldValue.getValue(), 1223.0);
        fieldValue.destroy();
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void RecursiveRelGetNodeAndRelList() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person)-[e:studyAt*1..1]->(b:organisation) WHERE a.fName = 'Alice' RETURN e;");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());

        KuzuValue nodeList = KuzuValueRecursiveRelUtil.getNodeList(value);
        assertTrue(nodeList.isOwnedByCPP());
        assertEquals(KuzuValueListUtil.getListSize(nodeList), 0);
        nodeList.destroy();

        KuzuValue relList = KuzuValueRecursiveRelUtil.getRelList(value);
        assertTrue(relList.isOwnedByCPP());
        assertEquals(KuzuValueListUtil.getListSize(relList), 1);

        KuzuValue rel = KuzuValueListUtil.getListElement(relList, 0);
        assertTrue(rel.isOwnedByCPP());
        KuzuInternalID srcId = KuzuValueRelUtil.getSrcID(rel);
        assertEquals(srcId.tableId, 0);
        assertEquals(srcId.offset, 0);

        KuzuInternalID dstId = KuzuValueRelUtil.getDstID(rel);
        assertEquals(dstId.tableId, 1);
        assertEquals(dstId.offset, 0);

        rel.destroy();
        relList.destroy();
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void RdfVariantTest() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:T_l) RETURN a.val ORDER BY a.id;");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        int i = 0;
        while (result.hasNext()) {
            KuzuFlatTuple flatTuple = result.getNext();
            KuzuValue value = flatTuple.getValue(0);
            KuzuDataType dataType = KuzuValueRdfVariantUtil.getDataType(value);
            switch (i) {
                case 0: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.INT64);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals(12L);
                    break;
                }
                case 1: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.INT32);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals(43);
                    break;
                }
                case 2: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.INT16);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals((short) 33);
                    break;
                }
                case 3: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.INT8);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals((byte) 2);
                    break;
                }
                case 4: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.UINT64);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals(BigInteger.valueOf(90));
                    break;
                }
                case 5: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.UINT32);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals(77);
                    break;
                }
                case 6: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.UINT16);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals((short) 12);
                    break;
                }
                case 7: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.UINT8);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals((byte) 1);
                    break;
                }
                case 8: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.DOUBLE);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals((double) 4.4);
                    break;
                }
                case 9: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.FLOAT);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals((float) 1.2);
                    break;
                }
                case 10: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.BOOL);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals(true);
                    break;
                }
                case 11: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.STRING);
                    assert KuzuValueRdfVariantUtil.getValue(value).equals("hhh");
                    break;
                }
                case 12: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.DATE);
                    LocalDate date = KuzuValueRdfVariantUtil.getValue(value);
                    assertEquals((long) date.toEpochDay(), 19723L);
                    break;
                }
                case 13: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.TIMESTAMP);
                    Instant stamp = KuzuValueRdfVariantUtil.getValue(value);
                    assertEquals(stamp.toEpochMilli(), 1704108330000L);
                    break;
                }
                case 14: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.INTERVAL);
                    Duration interval = KuzuValueRdfVariantUtil.getValue(value);
                    long month = (long) (interval.toDays() / 30);
                    long day = interval.toDays() - (long) (month * 30);
                    long micros = interval.minusDays(interval.toDays()).toMillis() * 1000;
                    assertEquals(month, 0);
                    assertEquals(day, 2);
                    assertEquals(micros, 0);
                    break;
                }
                case 15: {
                    assertEquals(dataType.getID(), KuzuDataTypeID.BLOB);
                    byte[] bytes = KuzuValueRdfVariantUtil.getValue(value);
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
