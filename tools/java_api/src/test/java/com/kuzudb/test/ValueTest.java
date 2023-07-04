package com.kuzudb.java_test;


import com.kuzudb.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.time.Duration;
import java.time.Instant;

public class ValueTest extends TestBase {

    @Test
    void ValueCreateNull() throws KuzuObjectRefDestroyedException {
        KuzuValue value = KuzuValue.createNull();
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.ANY);
        value.destroy();

        System.out.println("ValueCreateNull passed");
    }

    @Test
    void ValueCreateNullWithDatatype() throws KuzuObjectRefDestroyedException {
        KuzuDataType type = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        KuzuValue value = KuzuValue.createNullWithDataType(type);
        assertFalse(value.isOwnedByCPP());
        type.destroy();

        assertEquals(value.getDataType().getID(), KuzuDataTypeID.INT64);
        value.destroy();

        System.out.println("ValueCreateNullWithDatatype passed");
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

        System.out.println("ValueIsNull passed");
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

        System.out.println("ValueSetNull passed");
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

        System.out.println("ValueCreateDefault passed");
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
        assertEquals(id.table_id, 1);
        assertEquals(id.offset, 123);
        value.destroy();
    }

    @Test
    void ValueCreateNodeVal() throws KuzuObjectRefDestroyedException {
        // NodeVal
        KuzuInternalID id = new KuzuInternalID(1, 123);
        KuzuNodeValue nodeval = new KuzuNodeValue(id, "person");
        KuzuValue value = new KuzuValue(nodeval);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.NODE);
        KuzuNodeValue nv = value.getValue();
        assertEquals(nv.getID().table_id, 1);
        assertEquals(nv.getID().offset, 123);
        assertTrue(nv.getLabelName().equals("person"));
        assertEquals(nv.getPropertySize(), 0);
        nv.destroy();
        nodeval.destroy();
        value.destroy();
    }

    @Test
    void ValueCreateRelVal() throws KuzuObjectRefDestroyedException {
        // RelVal
        KuzuInternalID srcID = new KuzuInternalID(1, 123);
        KuzuInternalID dstID = new KuzuInternalID(2, 456);
        KuzuRelValue relval = new KuzuRelValue(srcID, dstID, "knows");
        KuzuValue value = new KuzuValue(relval);
        assertFalse(value.isOwnedByCPP());
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.REL);
        KuzuRelValue rv = value.getValue();
        assertEquals(rv.getSrcID().table_id, 1);
        assertEquals(rv.getSrcID().offset, 123);
        assertEquals(rv.getDstID().table_id, 2);
        assertEquals(rv.getDstID().offset, 456);
        assertTrue(rv.getLabelName().equals("knows"));
        assertEquals(rv.getPropertySize(), 0);
        value.destroy();
        relval.destroy();
        rv.destroy();
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

        System.out.println("ValueClone passed");
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

        System.out.println("ValueCopy passed");
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
        assertEquals(value.getListSize(), 2);

        value.destroy();
        flatTuple.destroy();
        result.destroy();

        System.out.println("ValueGetListSize passed");
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
        assertEquals(value.getListSize(), 2);

        KuzuValue listElement = value.getListElement(0);
        assertTrue(listElement.isOwnedByCPP());
        assertTrue(listElement.getValue().equals(10L));
        listElement.destroy();

        listElement = value.getListElement(1);
        assertTrue(listElement.isOwnedByCPP());
        assertTrue(listElement.getValue().equals(5L));
        listElement.destroy();

        listElement = value.getListElement(222);
        assertNull(listElement);

        value.destroy();
        flatTuple.destroy();
        result.destroy();

        System.out.println("ValueGetListElement passed");
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
        assertEquals(dataType.getID(), KuzuDataTypeID.VAR_LIST);
        KuzuDataType childDataType = dataType.getChildType();
        assertEquals(childDataType.getID(), KuzuDataTypeID.INT64);
        childDataType.destroy();
        dataType.destroy();
        value.destroy();

        flatTuple.destroy();
        result.destroy();

        System.out.println("ValueGetDatatype passed");
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
    void ValueGetInternalID() throws KuzuObjectRefDestroyedException {
        // InternalID
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        KuzuNodeValue nv = value.getValue();
        KuzuInternalID id = nv.getIDVal().getValue();
        assertEquals(id.table_id, 0);
        assertEquals(id.offset, 0);
        nv.destroy();
        value.destroy();
        flatTuple.destroy();
        result.destroy();
    }

    @Test
    void ValueGetRelVal() throws KuzuObjectRefDestroyedException {
        // RelVal
        KuzuQueryResult result = conn.query("MATCH (a:person) -[r:knows]-> (b:person) RETURN r ORDER BY a.ID, b.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        assertFalse(value.isNull());

        KuzuRelValue rel = value.getValue();
        KuzuInternalID srcId = rel.getSrcID();
        assertEquals(srcId.table_id, 0);
        assertEquals(srcId.offset, 0);

        KuzuInternalID dstId = rel.getDstID();
        assertEquals(dstId.table_id, 0);
        assertEquals(dstId.offset, 1);

        String label = rel.getLabelName();
        assertTrue(label.equals("knows"));
        long size = rel.getPropertySize();
        assertEquals(size, 5);

        rel.destroy();
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

        System.out.println("ValueToString passed");
    }

    @Test
    void NodeValClone() throws KuzuObjectRefDestroyedException {
        KuzuInternalID internalID = new KuzuInternalID(1, 123);
        KuzuNodeValue nodeVal = new KuzuNodeValue(internalID, "person");
        KuzuNodeValue nodeValClone = nodeVal.clone();
        nodeVal.destroy();

        assertEquals(nodeValClone.getID().table_id, 1);
        assertEquals(nodeValClone.getID().offset, 123);
        assertTrue(nodeValClone.getLabelName().equals("person"));
        assertEquals(nodeValClone.getPropertySize(), 0);

        nodeValClone.destroy();
        System.out.println("NodeValClone passed");
    }

    @Test
    void NodeValGetLabelVal() throws KuzuObjectRefDestroyedException {
        KuzuInternalID internalID = new KuzuInternalID(1, 123);
        KuzuNodeValue nodeVal = new KuzuNodeValue(internalID, "person");
        KuzuNodeValue nodeValClone = nodeVal.clone();
        nodeVal.destroy();

        assertEquals(nodeValClone.getID().table_id, 1);
        assertEquals(nodeValClone.getID().offset, 123);
        assertTrue(nodeValClone.getLabelName().equals("person"));
        assertEquals(nodeValClone.getPropertySize(), 0);

        nodeValClone.destroy();
        System.out.println("NodeValGetLabelVal passed");
    }

    @Test
    void NodeValNodeValGetID() throws KuzuObjectRefDestroyedException {
        KuzuInternalID internalID = new KuzuInternalID(1, 123);
        KuzuNodeValue nodeVal = new KuzuNodeValue(internalID, "person");
        KuzuValue value = new KuzuValue(nodeVal);
        KuzuInternalID nodeID = nodeVal.getID();
        assertEquals(nodeID.table_id, 1);
        assertEquals(nodeID.offset, 123);
        value.destroy();
        nodeVal.destroy();
        System.out.println("NodeValNodeValGetID passed");
    }

    @Test
    void NodeValGetLabelName() throws KuzuObjectRefDestroyedException {
        KuzuInternalID internalID = new KuzuInternalID(1, 123);
        KuzuNodeValue nodeVal = new KuzuNodeValue(internalID, "person");
        KuzuValue value = new KuzuValue(nodeVal);
        String labelName = nodeVal.getLabelName();
        assertTrue(labelName.equals("person"));
        value.destroy();
        nodeVal.destroy();
        System.out.println("NodeValGetLabelName passed");
    }

    @Test
    void NodeValAddProperty() throws KuzuObjectRefDestroyedException {
        KuzuInternalID internalID = new KuzuInternalID(1, 123);
        KuzuNodeValue nodeVal = new KuzuNodeValue(internalID, "person");
        long propertySize = nodeVal.getPropertySize();
        assertEquals(propertySize, 0);

        String propertyKey = "fName";
        KuzuValue propertyValue = new KuzuValue("Alice");
        nodeVal.addProperty(propertyKey, propertyValue);
        propertySize = nodeVal.getPropertySize();
        assertEquals(propertySize, 1);
        propertyValue.destroy();

        propertyKey = "age";
        propertyValue = new KuzuValue(10);
        nodeVal.addProperty(propertyKey, propertyValue);
        propertySize = nodeVal.getPropertySize();
        assertEquals(propertySize, 2);

        propertyValue.destroy();
        nodeVal.destroy();

        System.out.println("NodeValAddProperty passed");
    }

    @Test
    void NodeValGetProperty() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a ORDER BY a.ID");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        KuzuValue value = flatTuple.getValue(0);
        assertTrue(value.isOwnedByCPP());
        KuzuNodeValue node = value.getValue();
        String propertyName = node.getPropertyNameAt(0);
        assertTrue(propertyName.equals("ID"));
        propertyName = node.getPropertyNameAt(1);
        assertTrue(propertyName.equals("fName"));
        propertyName = node.getPropertyNameAt(2);
        assertTrue(propertyName.equals("gender"));
        propertyName = node.getPropertyNameAt(3);
        assertTrue(propertyName.equals("isStudent"));

        KuzuValue propertyValue = node.getPropertyValueAt(0);
        long propertyValueID = propertyValue.getValue();
        assertEquals(propertyValueID, 0);
        propertyValue.destroy();
        propertyValue = node.getPropertyValueAt(1);
        String propertyValuefName = propertyValue.getValue();
        assertTrue(propertyValuefName.equals("Alice"));
        propertyValue.destroy();
        propertyValue = node.getPropertyValueAt(2);
        long propertyValueGender = propertyValue.getValue();
        assertEquals(propertyValueGender, 1);
        propertyValue.destroy();
        propertyValue = node.getPropertyValueAt(3);
        boolean propertyValueIsStudent = propertyValue.getValue();
        assertEquals(propertyValueIsStudent, true);
        propertyValue.destroy();

        node.destroy();
        value.destroy();
        flatTuple.destroy();
        result.destroy();

        System.out.println("NodeValGetProperty passed");
    }

    @Test
    void NodeValToString() throws KuzuObjectRefDestroyedException {
        KuzuInternalID internalID = new KuzuInternalID(1, 123);
        KuzuNodeValue nodeVal = new KuzuNodeValue(internalID, "person");

        String propertyKey = "fName";
        KuzuValue propertyValue = new KuzuValue("Smith");
        nodeVal.addProperty(propertyKey, propertyValue);
        propertyValue.destroy();

        propertyKey = "age";
        propertyValue = new KuzuValue(10L);
        nodeVal.addProperty(propertyKey, propertyValue);
        propertyValue.destroy();

        String str = nodeVal.toString();
        System.out.println(str);
        assertTrue(str.equals("(label:person, 1:123, {fName:Smith, age:10})"));
        nodeVal.destroy();

        System.out.println("NodeValToString passed");
    }

    @Test
    void RelValClone() throws KuzuObjectRefDestroyedException {
        KuzuInternalID srcID = new KuzuInternalID(1, 123);
        KuzuInternalID dstID = new KuzuInternalID(2, 456);
        KuzuRelValue relVal = new KuzuRelValue(srcID, dstID, "knows");
        KuzuRelValue relValClone = relVal.clone();
        relVal.destroy();
        KuzuValue srcIDVal = relValClone.getSrcIDVal();
        KuzuValue dstIDVal = relValClone.getDstIDVal();
        KuzuInternalID srcIDClone = srcIDVal.getValue();
        KuzuInternalID dstIDClone = dstIDVal.getValue();
        assertEquals(srcIDClone.table_id, srcID.table_id);
        assertEquals(srcIDClone.offset, srcID.offset);
        assertEquals(dstIDClone.table_id, dstID.table_id);
        assertEquals(dstIDClone.offset, dstID.offset);
        String labelName = relValClone.getLabelName();
        assertTrue(labelName.equals("knows"));
        long propertySize = relValClone.getPropertySize();
        assertEquals(propertySize, 0);
        srcIDVal.destroy();
        dstIDVal.destroy();
        relValClone.destroy();

        System.out.println("RelValClone passed");
    }

    @Test
    void RelValAddAndGetProperty() throws KuzuObjectRefDestroyedException {
        KuzuInternalID srcID = new KuzuInternalID(1, 123);
        KuzuInternalID dstID = new KuzuInternalID(2, 456);
        KuzuRelValue relVal = new KuzuRelValue(srcID, dstID, "knows");
        long propertySize = relVal.getPropertySize();
        assertEquals(propertySize, 0);

        String propertyKey = "fName";
        KuzuValue propertyValue = new KuzuValue("Alice");
        relVal.addProperty(propertyKey, propertyValue);
        propertySize = relVal.getPropertySize();
        assertEquals(propertySize, 1);
        propertyValue.destroy();

        propertyKey = "age";
        propertyValue = new KuzuValue(10L);
        relVal.addProperty(propertyKey, propertyValue);
        propertySize = relVal.getPropertySize();
        assertEquals(propertySize, 2);
        propertyValue.destroy();

        propertyKey = relVal.getPropertyNameAt(0);
        assertTrue(propertyKey.equals("fName"));

        propertyKey = relVal.getPropertyNameAt(1);
        assertTrue(propertyKey.equals("age"));

        propertyValue = relVal.getPropertyValueAt(0);
        String propertyValuefName = propertyValue.getValue();
        assertTrue(propertyValuefName.equals("Alice"));
        propertyValue.destroy();

        propertyValue = relVal.getPropertyValueAt(1);
        long propertyValueAge = propertyValue.getValue();
        assertEquals(propertyValueAge, 10L);
        propertyValue.destroy();

        relVal.destroy();
        System.out.println("RelValAddAndGetProperty passed");
    }

    @Test
    void RelValToString() throws KuzuObjectRefDestroyedException {
        KuzuInternalID srcID = new KuzuInternalID(1, 123);
        KuzuInternalID dstID = new KuzuInternalID(2, 456);
        KuzuRelValue relVal = new KuzuRelValue(srcID, dstID, "knows");

        String propertyKey = "fName";
        KuzuValue propertyValue = new KuzuValue("Alice");
        relVal.addProperty(propertyKey, propertyValue);
        propertyValue.destroy();

        propertyKey = "age";
        propertyValue = new KuzuValue(10L);
        relVal.addProperty(propertyKey, propertyValue);
        propertyValue.destroy();

        String str = relVal.toString();
        assertTrue(str.equals("(1:123)-[label:knows, {fName:Alice, age:10}]->(2:456)"));

        relVal.destroy();
        System.out.println("RelValToString passed");
    }
}
