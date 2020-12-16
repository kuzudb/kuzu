
#include "gtest/gtest.h"

#include "src/common/include/vector/property_vector.h"

TEST(PropertyVectorTests, CreateBooleanFlatListTest) {
    auto vector = PropertyVector<uint8_t>();
    vector.reset();
    for (uint64_t i = 0; i < NODE_SEQUENCE_VECTOR_SIZE; i++) {
        if (i < 100) {
            vector.set(i, TRUE);
        } else if (i >= 100 && i < 666) {
            vector.set(i, FALSE);
        } else {
            vector.set(i, NULL_BOOL);
        }
    }
    uint8_t bool_val;
    for (uint64_t i = 0; i < NODE_SEQUENCE_VECTOR_SIZE; i++) {
        vector.get(i, bool_val);
        if (i < 100) {
            ASSERT_EQ(bool_val, TRUE);
        } else if (i >= 100 && i < 666) {
            ASSERT_EQ(bool_val, FALSE);
        } else {
            ASSERT_EQ(bool_val, NULL_BOOL);
        }
    }
}

TEST(PropertyVectorTests, CreateIntegerPropertyVectorTest) {
    auto vector = PropertyVector<int32_t>();
    vector.reset();
    for (uint64_t i = 0; i < NODE_SEQUENCE_VECTOR_SIZE; i++) {
        vector.set(i, 42 + i);
    }
    int32_t int_value;
    for (uint64_t i = 0; i < NODE_SEQUENCE_VECTOR_SIZE; i++) {
        vector.get(i, int_value);
        ASSERT_EQ(int_value, 42 + i);
    }
}

TEST(PropertyVectorTests, CreateDoublePropertyVectorTest) {
    auto vector = PropertyVector<double_t>();
    vector.reset();
    for (uint64_t i = 0; i < NODE_SEQUENCE_VECTOR_SIZE; i++) {
        vector.set(i, 42.5 + i);
    }
    double_t double_value;
    for (uint64_t i = 0; i < NODE_SEQUENCE_VECTOR_SIZE; i++) {
        vector.get(i, double_value);
        ASSERT_EQ(double_value, 42.5 + i);
    }
}
