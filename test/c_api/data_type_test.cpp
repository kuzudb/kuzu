#include "c_api/kuzu.h"
#include "common/types/types.h"
#include "gtest/gtest.h"

using namespace kuzu::common;

TEST(CApiDataTypeTest, Create) {
    kuzu_logical_type dataType;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0, &dataType);
    ASSERT_NE(dataType._data_type, nullptr);
    auto dataTypeCpp = (LogicalType*)dataType._data_type;
    ASSERT_EQ(dataTypeCpp->getLogicalTypeID(), LogicalTypeID::INT64);

    kuzu_logical_type dataType2;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_LIST, &dataType, 0, &dataType2);
    ASSERT_NE(dataType2._data_type, nullptr);
    auto dataTypeCpp2 = (LogicalType*)dataType2._data_type;
    ASSERT_EQ(dataTypeCpp2->getLogicalTypeID(), LogicalTypeID::LIST);
    // ASSERT_EQ(dataTypeCpp2->getChildType()->getLogicalTypeID(), LogicalTypeID::INT64);

    kuzu_logical_type dataType3;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_ARRAY, &dataType, 100, &dataType3);
    ASSERT_NE(dataType3._data_type, nullptr);
    auto dataTypeCpp3 = (LogicalType*)dataType3._data_type;
    ASSERT_EQ(dataTypeCpp3->getLogicalTypeID(), LogicalTypeID::ARRAY);
    // ASSERT_EQ(dataTypeCpp3->getChildType()->getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(ArrayType::getNumElements(*dataTypeCpp3), 100);

    // Since child type is copied, we should be able to destroy the original type without an error.
    kuzu_data_type_destroy(&dataType);
    kuzu_data_type_destroy(&dataType2);
    kuzu_data_type_destroy(&dataType3);
}

TEST(CApiDataTypeTest, Clone) {
    kuzu_logical_type dataType;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0, &dataType);
    ASSERT_NE(dataType._data_type, nullptr);
    kuzu_logical_type dataTypeClone;
    kuzu_data_type_clone(&dataType, &dataTypeClone);
    ASSERT_NE(dataTypeClone._data_type, nullptr);
    auto dataTypeCpp = (LogicalType*)dataType._data_type;
    auto dataTypeCloneCpp = (LogicalType*)dataTypeClone._data_type;
    ASSERT_TRUE(*dataTypeCpp == *dataTypeCloneCpp);

    kuzu_logical_type dataType2;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_LIST, &dataType, 0, &dataType2);
    ASSERT_NE(dataType2._data_type, nullptr);
    kuzu_logical_type dataTypeClone2;
    kuzu_data_type_clone(&dataType2, &dataTypeClone2);
    ASSERT_NE(dataTypeClone2._data_type, nullptr);
    auto dataTypeCpp2 = (LogicalType*)dataType2._data_type;
    auto dataTypeCloneCpp2 = (LogicalType*)dataTypeClone2._data_type;
    ASSERT_TRUE(*dataTypeCpp2 == *dataTypeCloneCpp2);

    kuzu_logical_type dataType3;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_ARRAY, &dataType, 100, &dataType3);
    ASSERT_NE(dataType3._data_type, nullptr);
    kuzu_logical_type dataTypeClone3;
    kuzu_data_type_clone(&dataType3, &dataTypeClone3);
    ASSERT_NE(dataTypeClone3._data_type, nullptr);
    auto dataTypeCpp3 = (LogicalType*)dataType3._data_type;
    auto dataTypeCloneCpp3 = (LogicalType*)dataTypeClone3._data_type;
    ASSERT_TRUE(*dataTypeCpp3 == *dataTypeCloneCpp3);

    kuzu_data_type_destroy(&dataType);
    kuzu_data_type_destroy(&dataType2);
    kuzu_data_type_destroy(&dataType3);
    kuzu_data_type_destroy(&dataTypeClone);
    kuzu_data_type_destroy(&dataTypeClone2);
    kuzu_data_type_destroy(&dataTypeClone3);
}

TEST(CApiDataTypeTest, Equals) {
    kuzu_logical_type dataType;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0, &dataType);
    ASSERT_NE(dataType._data_type, nullptr);
    kuzu_logical_type dataTypeClone;
    kuzu_data_type_clone(&dataType, &dataTypeClone);
    ASSERT_NE(dataTypeClone._data_type, nullptr);
    ASSERT_TRUE(kuzu_data_type_equals(&dataType, &dataTypeClone));

    kuzu_logical_type dataType2;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_LIST, &dataType, 0, &dataType2);
    ASSERT_NE(dataType2._data_type, nullptr);
    kuzu_logical_type dataTypeClone2;
    kuzu_data_type_clone(&dataType2, &dataTypeClone2);
    ASSERT_NE(dataTypeClone2._data_type, nullptr);
    ASSERT_TRUE(kuzu_data_type_equals(&dataType2, &dataTypeClone2));

    kuzu_logical_type dataType3;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_ARRAY, &dataType, 100, &dataType3);
    ASSERT_NE(dataType3._data_type, nullptr);
    kuzu_logical_type dataTypeClone3;
    kuzu_data_type_clone(&dataType3, &dataTypeClone3);
    ASSERT_NE(dataTypeClone3._data_type, nullptr);
    ASSERT_TRUE(kuzu_data_type_equals(&dataType3, &dataTypeClone3));

    ASSERT_FALSE(kuzu_data_type_equals(&dataType, &dataType2));
    ASSERT_FALSE(kuzu_data_type_equals(&dataType, &dataType3));
    ASSERT_FALSE(kuzu_data_type_equals(&dataType2, &dataType3));

    kuzu_data_type_destroy(&dataType);
    kuzu_data_type_destroy(&dataType2);
    kuzu_data_type_destroy(&dataType3);
    kuzu_data_type_destroy(&dataTypeClone);
    kuzu_data_type_destroy(&dataTypeClone2);
    kuzu_data_type_destroy(&dataTypeClone3);
}

TEST(CApiDataTypeTest, GetID) {
    kuzu_logical_type dataType;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0, &dataType);
    ASSERT_NE(dataType._data_type, nullptr);
    ASSERT_EQ(kuzu_data_type_get_id(&dataType), kuzu_data_type_id::KUZU_INT64);

    kuzu_logical_type dataType2;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_LIST, &dataType, 0, &dataType2);
    ASSERT_NE(dataType2._data_type, nullptr);
    ASSERT_EQ(kuzu_data_type_get_id(&dataType2), kuzu_data_type_id::KUZU_LIST);

    kuzu_logical_type dataType3;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_ARRAY, &dataType, 100, &dataType3);
    ASSERT_NE(dataType3._data_type, nullptr);
    ASSERT_EQ(kuzu_data_type_get_id(&dataType3), kuzu_data_type_id::KUZU_ARRAY);

    kuzu_data_type_destroy(&dataType);
    kuzu_data_type_destroy(&dataType2);
    kuzu_data_type_destroy(&dataType3);
}

// TODO(Chang): The getChildType interface has been removed from the C++ DataType class.
// Consider adding the StructType/ListType helper to C binding.
// TEST(CApiDataTypeTest, GetChildType) {
//    auto dataType = kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0);
//    ASSERT_NE(dataType, nullptr);
//    ASSERT_EQ(kuzu_data_type_get_child_type(dataType), nullptr);
//
//    auto dataType2 = kuzu_data_type_create(kuzu_data_type_id::KUZU_LIST, dataType, 0);
//    ASSERT_NE(dataType2, nullptr);
//    auto childType2 = kuzu_data_type_get_child_type(dataType2);
//    ASSERT_NE(childType2, nullptr);
//    ASSERT_EQ(kuzu_data_type_get_id(childType2), kuzu_data_type_id::KUZU_INT64);
//    kuzu_data_type_destroy(childType2);
//    kuzu_data_type_destroy(dataType2);
//
//    auto dataType3 = kuzu_data_type_create(kuzu_data_type_id::KUZU_ARRAY, dataType, 100);
//    ASSERT_NE(dataType3, nullptr);
//    auto childType3 = kuzu_data_type_get_child_type(dataType3);
//    kuzu_data_type_destroy(dataType3);
//    // Destroying dataType3 should not destroy childType3.
//    ASSERT_NE(childType3, nullptr);
//    ASSERT_EQ(kuzu_data_type_get_id(childType3), kuzu_data_type_id::KUZU_INT64);
//    kuzu_data_type_destroy(childType3);
//
//    kuzu_data_type_destroy(dataType);
//}

TEST(CApiDataTypeTest, GetFixedNumElementsInList) {
    kuzu_logical_type dataType;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0, &dataType);
    ASSERT_NE(dataType._data_type, nullptr);
    uint64_t numElements;
    ASSERT_EQ(kuzu_data_type_get_num_elements_in_array(&dataType, &numElements), KuzuError);

    kuzu_logical_type dataType2;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_LIST, &dataType, 0, &dataType2);
    ASSERT_NE(dataType2._data_type, nullptr);
    ASSERT_EQ(kuzu_data_type_get_num_elements_in_array(&dataType2, &numElements), KuzuError);

    kuzu_logical_type dataType3;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_ARRAY, &dataType, 100, &dataType3);
    ASSERT_NE(dataType3._data_type, nullptr);
    ASSERT_EQ(kuzu_data_type_get_num_elements_in_array(&dataType3, &numElements), KuzuSuccess);
    ASSERT_EQ(numElements, 100);

    kuzu_data_type_destroy(&dataType);
    kuzu_data_type_destroy(&dataType2);
    kuzu_data_type_destroy(&dataType3);
}
