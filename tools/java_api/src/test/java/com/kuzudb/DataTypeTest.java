package com.kuzudb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class DataTypeTest extends TestBase {

    @Test
    void DataTypeClone() {
        DataType dataType = new DataType(DataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        DataType dataTypeClone = dataType.clone();
        assertNotNull(dataTypeClone);
        assertEquals(dataTypeClone.getID(), DataTypeID.INT64);

        DataType dataType2 = new DataType(DataTypeID.LIST, dataType, 0);
        assertNotNull(dataType2);
        DataType dataTypeClone2 = dataType2.clone();
        assertNotNull(dataTypeClone2);
        assertEquals(dataTypeClone2.getID(), DataTypeID.LIST);
        assertEquals(dataTypeClone2.getChildType().getID(), DataTypeID.INT64);

        DataType dataType3 = new DataType(DataTypeID.ARRAY, dataType, 100);
        assertNotNull(dataType3);
        DataType dataTypeClone3 = dataType3.clone();
        assertNotNull(dataTypeClone3);
        assertEquals(dataTypeClone3.getID(), DataTypeID.ARRAY);
        assertEquals(dataTypeClone3.getChildType().getID(), DataTypeID.INT64);
        assertEquals(dataTypeClone3.getFixedNumElementsInList(), 100);

        assertFalse(dataTypeClone2.equals(dataTypeClone3));
        dataType.close();
        dataType2.close();
        dataType3.close();
        dataTypeClone.close();
        dataTypeClone2.close();
        dataTypeClone3.close();
    }

    @Test
    void DataTypeEquals() {
        DataType dataType = new DataType(DataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        DataType dataTypeClone = dataType.clone();
        assertNotNull(dataTypeClone);
        assertTrue(dataType.equals(dataTypeClone));

        DataType dataType2 = new DataType(DataTypeID.LIST, dataType, 0);
        assertNotNull(dataType2);
        DataType dataTypeClone2 = dataType2.clone();
        assertNotNull(dataTypeClone2);
        assertTrue(dataType2.equals(dataTypeClone2));

        DataType dataType3 = new DataType(DataTypeID.ARRAY, dataType, 100);
        assertNotNull(dataType3);
        DataType dataTypeClone3 = dataType3.clone();
        assertNotNull(dataTypeClone3);
        assertTrue(dataType3.equals(dataTypeClone3));

        assertFalse(dataType.equals(dataType2));
        assertFalse(dataType.equals(dataType3));
        assertFalse(dataType2.equals(dataType3));
        assertFalse(dataTypeClone.equals(dataTypeClone3));

        dataType.close();
        dataType2.close();
        dataType3.close();
        dataTypeClone.close();
        dataTypeClone2.close();
        dataTypeClone3.close();
    }

    @Test
    void DataTypeGetID() {
        DataType dataType = new DataType(DataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        assertEquals(dataType.getID(), DataTypeID.INT64);

        DataType dataType2 = new DataType(DataTypeID.LIST, dataType, 0);
        assertNotNull(dataType2);
        assertEquals(dataType2.getID(), DataTypeID.LIST);

        DataType dataType3 = new DataType(DataTypeID.ARRAY, dataType, 100);
        assertNotNull(dataType3);
        assertEquals(dataType3.getID(), DataTypeID.ARRAY);

        dataType.close();
        dataType2.close();
        dataType3.close();
    }

    @Test
    void DataTypeGetChildType() {
        DataType dataType = new DataType(DataTypeID.INT64, null, 0);
        assertNotNull(dataType);

        DataType dataType2 = new DataType(DataTypeID.LIST, dataType, 0);
        assertNotNull(dataType2);
        assertEquals(dataType2.getChildType().getID(), DataTypeID.INT64);

        DataType dataType3 = new DataType(DataTypeID.ARRAY, dataType, 100);
        assertNotNull(dataType3);
        assertEquals(dataType3.getChildType().getID(), DataTypeID.INT64);

        dataType.close();
        dataType2.close();
        dataType3.close();
    }

    @Test
    void DataTypeGetFixedNumElementsInList() {
        DataType dataType = new DataType(DataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        assertEquals(dataType.getFixedNumElementsInList(), 0);

        DataType dataType2 = new DataType(DataTypeID.LIST, dataType, 0);
        assertNotNull(dataType2);
        assertEquals(dataType.getFixedNumElementsInList(), 0);

        DataType dataType3 = new DataType(DataTypeID.ARRAY, dataType, 100);
        assertNotNull(dataType3);
        assertEquals(dataType3.getFixedNumElementsInList(), 100);
    }

    @Test
    void DataTypeTestDestroy() {
        try (DataType dataType = new DataType(DataTypeID.INT64, null, 0)) {
            assertNotNull(dataType);
            assertEquals(dataType.getFixedNumElementsInList(), 0);

            try (DataType dataType2 = new DataType(DataTypeID.LIST, dataType, 0)) {
                assertNotNull(dataType2);
                assertEquals(dataType2.getFixedNumElementsInList(), 0);
            }

            try (DataType dataType3 = new DataType(DataTypeID.ARRAY, dataType, 100)) {
                assertNotNull(dataType3);
                assertEquals(dataType3.getFixedNumElementsInList(), 100);
            }
        }
    }
}
