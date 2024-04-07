package com.kuzudb.java_test;

import com.kuzudb.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class DataTypeTest extends TestBase {

    @Test
    void DataTypeClone() throws KuzuObjectRefDestroyedException {
        KuzuDataType dataType = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        KuzuDataType dataTypeClone = dataType.clone();
        assertNotNull(dataTypeClone);
        assertEquals(dataTypeClone.getID(), KuzuDataTypeID.INT64);

        KuzuDataType dataType2 = new KuzuDataType(KuzuDataTypeID.LIST, dataType, 0);
        assertNotNull(dataType2);
        KuzuDataType dataTypeClone2 = dataType2.clone();
        assertNotNull(dataTypeClone2);
        assertEquals(dataTypeClone2.getID(), KuzuDataTypeID.LIST);
        assertEquals(dataTypeClone2.getChildType().getID(), KuzuDataTypeID.INT64);

        KuzuDataType dataType3 = new KuzuDataType(KuzuDataTypeID.ARRAY, dataType, 100);
        assertNotNull(dataType3);
        KuzuDataType dataTypeClone3 = dataType3.clone();
        assertNotNull(dataTypeClone3);
        assertEquals(dataTypeClone3.getID(), KuzuDataTypeID.ARRAY);
        assertEquals(dataTypeClone3.getChildType().getID(), KuzuDataTypeID.INT64);
        assertEquals(dataTypeClone3.getFixedNumElementsInList(), 100);

        assertFalse(dataTypeClone2.equals(dataTypeClone3));

        dataType.destroy();
        dataType2.destroy();
        dataType3.destroy();
        dataTypeClone.destroy();
        dataTypeClone2.destroy();
        dataTypeClone3.destroy();
    }

    @Test
    void DataTypeEquals() throws KuzuObjectRefDestroyedException {
        KuzuDataType dataType = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        KuzuDataType dataTypeClone = dataType.clone();
        assertNotNull(dataTypeClone);
        assertTrue(dataType.equals(dataTypeClone));

        KuzuDataType dataType2 = new KuzuDataType(KuzuDataTypeID.LIST, dataType, 0);
        assertNotNull(dataType2);
        KuzuDataType dataTypeClone2 = dataType2.clone();
        assertNotNull(dataTypeClone2);
        assertTrue(dataType2.equals(dataTypeClone2));

        KuzuDataType dataType3 = new KuzuDataType(KuzuDataTypeID.ARRAY, dataType, 100);
        assertNotNull(dataType3);
        KuzuDataType dataTypeClone3 = dataType3.clone();
        assertNotNull(dataTypeClone3);
        assertTrue(dataType3.equals(dataTypeClone3));

        assertFalse(dataType.equals(dataType2));
        assertFalse(dataType.equals(dataType3));
        assertFalse(dataType2.equals(dataType3));
        assertFalse(dataTypeClone.equals(dataTypeClone3));

        dataType.destroy();
        dataType2.destroy();
        dataType3.destroy();
        dataTypeClone.destroy();
        dataTypeClone2.destroy();
        dataTypeClone3.destroy();
    }

    @Test
    void DataTypeGetID() throws KuzuObjectRefDestroyedException {
        KuzuDataType dataType = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        assertEquals(dataType.getID(), KuzuDataTypeID.INT64);

        KuzuDataType dataType2 = new KuzuDataType(KuzuDataTypeID.LIST, dataType, 0);
        assertNotNull(dataType2);
        assertEquals(dataType2.getID(), KuzuDataTypeID.LIST);

        KuzuDataType dataType3 = new KuzuDataType(KuzuDataTypeID.ARRAY, dataType, 100);
        assertNotNull(dataType3);
        assertEquals(dataType3.getID(), KuzuDataTypeID.ARRAY);

        dataType.destroy();
        dataType2.destroy();
        dataType3.destroy();
    }

    @Test
    void DataTypeGetChildType() throws KuzuObjectRefDestroyedException {
        KuzuDataType dataType = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        assertNotNull(dataType);

        KuzuDataType dataType2 = new KuzuDataType(KuzuDataTypeID.LIST, dataType, 0);
        assertNotNull(dataType2);
        assertEquals(dataType2.getChildType().getID(), KuzuDataTypeID.INT64);

        KuzuDataType dataType3 = new KuzuDataType(KuzuDataTypeID.ARRAY, dataType, 100);
        assertNotNull(dataType3);
        assertEquals(dataType3.getChildType().getID(), KuzuDataTypeID.INT64);

        dataType.destroy();
        dataType2.destroy();
        dataType3.destroy();
    }

    @Test
    void DataTypeGetFixedNumElementsInList() throws KuzuObjectRefDestroyedException {
        KuzuDataType dataType = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        assertEquals(dataType.getFixedNumElementsInList(), 0);

        KuzuDataType dataType2 = new KuzuDataType(KuzuDataTypeID.LIST, dataType, 0);
        assertNotNull(dataType2);
        assertEquals(dataType.getFixedNumElementsInList(), 0);

        KuzuDataType dataType3 = new KuzuDataType(KuzuDataTypeID.ARRAY, dataType, 100);
        assertNotNull(dataType3);
        assertEquals(dataType3.getFixedNumElementsInList(), 100);

        dataType.destroy();
        dataType2.destroy();
        dataType3.destroy();
    }
}
