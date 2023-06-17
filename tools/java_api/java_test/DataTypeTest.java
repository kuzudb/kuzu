package tools.java_api.java_test;

import tools.java_api.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


public class DataTypeTest {

    @Test
    void DataTypeClone() {
        KuzuDataType dataType = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        KuzuDataType dataTypeClone = dataType.clone();
        assertNotNull(dataTypeClone);
        assertEquals(dataTypeClone.getID(), KuzuDataTypeID.INT64);

        KuzuDataType dataType2 = new KuzuDataType(KuzuDataTypeID.VAR_LIST, dataType, 0);
        assertNotNull(dataType2);
        KuzuDataType dataTypeClone2 = dataType2.clone();
        assertNotNull(dataTypeClone2);
        assertEquals(dataTypeClone2.getID(), KuzuDataTypeID.VAR_LIST);
        assertEquals(dataTypeClone2.getChildType().getID(), KuzuDataTypeID.INT64);

        KuzuDataType dataType3 = new KuzuDataType(KuzuDataTypeID.FIXED_LIST, dataType, 100);
        assertNotNull(dataType3);
        KuzuDataType dataTypeClone3 = dataType3.clone();
        assertNotNull(dataTypeClone3);
        assertEquals(dataTypeClone3.getID(), KuzuDataTypeID.FIXED_LIST);
        assertEquals(dataTypeClone3.getChildType().getID(), KuzuDataTypeID.INT64);
        assertEquals(dataTypeClone3.getFixedNumElementsInList(), 100);

        assertFalse(dataTypeClone2.equals(dataTypeClone3));
        
        dataType.destory();
        dataType2.destory();
        dataType3.destory();
        dataTypeClone.destory();
        dataTypeClone2.destory();
        dataTypeClone3.destory();

        System.out.println("DataTypeClone passed");
    }

    @Test
    void DataTypeEquals() {
        KuzuDataType dataType = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        KuzuDataType dataTypeClone = dataType.clone();
        assertNotNull(dataTypeClone);
        assertTrue(dataType.equals(dataTypeClone));

        KuzuDataType dataType2 = new KuzuDataType(KuzuDataTypeID.VAR_LIST, dataType, 0);
        assertNotNull(dataType2);
        KuzuDataType dataTypeClone2 = dataType2.clone();
        assertNotNull(dataTypeClone2);
        assertTrue(dataType2.equals(dataTypeClone2));

        KuzuDataType dataType3 = new KuzuDataType(KuzuDataTypeID.FIXED_LIST, dataType, 100);
        assertNotNull(dataType3);
        KuzuDataType dataTypeClone3 = dataType3.clone();
        assertNotNull(dataTypeClone3);
        assertTrue(dataType3.equals(dataTypeClone3));

        assertFalse(dataType.equals(dataType2));
        assertFalse(dataType.equals(dataType3));
        assertFalse(dataType2.equals(dataType3));
        assertFalse(dataTypeClone.equals(dataTypeClone3));
        
        dataType.destory();
        dataType2.destory();
        dataType3.destory();
        dataTypeClone.destory();
        dataTypeClone2.destory();
        dataTypeClone3.destory();

        System.out.println("DataTypeEquals passed");
    }

    @Test
    void DataTypeGetID() {
        KuzuDataType dataType = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        assertEquals(dataType.getID(), KuzuDataTypeID.INT64);

        KuzuDataType dataType2 = new KuzuDataType(KuzuDataTypeID.VAR_LIST, dataType, 0);
        assertNotNull(dataType2);
        assertEquals(dataType2.getID(), KuzuDataTypeID.VAR_LIST);

        KuzuDataType dataType3 = new KuzuDataType(KuzuDataTypeID.FIXED_LIST, dataType, 100);
        assertNotNull(dataType3);
        assertEquals(dataType3.getID(), KuzuDataTypeID.FIXED_LIST);

        dataType.destory();
        dataType2.destory();
        dataType3.destory();

        System.out.println("DataTypeGetID passed");
    }

    @Test
    void DataTypeGetChildType() {
        KuzuDataType dataType = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        assertNotNull(dataType);

        KuzuDataType dataType2 = new KuzuDataType(KuzuDataTypeID.VAR_LIST, dataType, 0);
        assertNotNull(dataType2);
        assertEquals(dataType2.getChildType().getID(), KuzuDataTypeID.INT64);

        KuzuDataType dataType3 = new KuzuDataType(KuzuDataTypeID.FIXED_LIST, dataType, 100);
        assertNotNull(dataType3);
        assertEquals(dataType3.getChildType().getID(), KuzuDataTypeID.INT64);

        dataType.destory();
        dataType2.destory();
        dataType3.destory();

        System.out.println("DataTypeGetChildType passed");
    }

    @Test 
    void DataTypeGetFixedNumElementsInList() {
        KuzuDataType dataType = new KuzuDataType(KuzuDataTypeID.INT64, null, 0);
        assertNotNull(dataType);
        assertEquals(dataType.getFixedNumElementsInList(), 0);

        KuzuDataType dataType2 = new KuzuDataType(KuzuDataTypeID.VAR_LIST, dataType, 0);
        assertNotNull(dataType2);
        assertEquals(dataType.getFixedNumElementsInList(), 0);

        KuzuDataType dataType3 = new KuzuDataType(KuzuDataTypeID.FIXED_LIST, dataType, 100);
        assertNotNull(dataType3);
        assertEquals(dataType3.getFixedNumElementsInList(), 100);

        dataType.destory();
        dataType2.destory();
        dataType3.destory();

        System.out.println("DataTypeGetFixedNumElementsInList passed");

    }
}
