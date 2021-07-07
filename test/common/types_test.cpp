#include <string>

#include "include/gtest/gtest.h"

#include "src/common/include/exception.h"
#include "src/common/include/types.h"

using namespace graphflow::common;
using namespace std;

TEST(TypesTests, StringToINT64Conversion) {
    EXPECT_EQ(2147483648, TypeUtils::TypeUtils::convertToInt64("2147483648"));
    EXPECT_EQ(-2147483648, TypeUtils::TypeUtils::convertToInt64("-2147483648"));
    EXPECT_EQ(2147483648000, TypeUtils::TypeUtils::convertToInt64("2147483648000"));
    EXPECT_EQ(-2147483648000, TypeUtils::TypeUtils::convertToInt64("-2147483648000"));
    EXPECT_EQ(648, TypeUtils::TypeUtils::convertToInt64("648"));
    EXPECT_EQ(-648, TypeUtils::TypeUtils::convertToInt64("-648"));
    EXPECT_EQ(0, TypeUtils::TypeUtils::convertToInt64("0"));
    EXPECT_EQ(0, TypeUtils::TypeUtils::convertToInt64("-0"));
}

TEST(TypesTests, StringToINT64ConversionErrors) {
    // Max overflow
    try {
        int64_t result = TypeUtils::TypeUtils::convertToInt64("2147483648000000000000");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Min underflow
    try {
        int64_t result = TypeUtils::TypeUtils::convertToInt64("-2147483648000000000000");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Wrong input
    try {
        int64_t result = TypeUtils::TypeUtils::convertToInt64("qq1244");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Empty input
    try {
        int64_t result = TypeUtils::TypeUtils::convertToInt64("");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        int64_t result = TypeUtils::TypeUtils::convertToInt64("24x[xd432");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        int64_t result = TypeUtils::TypeUtils::convertToInt64("0L");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }
}

TEST(TypesTests, StringToDoubleConversion) {
    EXPECT_EQ(12235.14, TypeUtils::TypeUtils::convertToDouble("12235.14"));
    EXPECT_EQ(-12235.14013, TypeUtils::TypeUtils::convertToDouble("-12235.14013"));
    EXPECT_EQ(0.001, TypeUtils::TypeUtils::convertToDouble("0.001"));
    EXPECT_EQ(-0.001, TypeUtils::TypeUtils::convertToDouble("-0.001"));
    EXPECT_EQ(0.0, TypeUtils::TypeUtils::convertToDouble("0.0"));
    EXPECT_EQ(0.0, TypeUtils::TypeUtils::convertToDouble("-0.0"));
}

TEST(TypesTests, StringToDoubleConversionErrors) {
    // Wrong input
    try {
        double_t result = TypeUtils::TypeUtils::convertToDouble("x2.4r432");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Empty input
    try {
        double_t result = TypeUtils::TypeUtils::convertToDouble("");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        double_t result = TypeUtils::TypeUtils::convertToDouble("2.4r432");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        double_t result = TypeUtils::TypeUtils::convertToDouble("0.0f");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }
}

TEST(TypesTests, StringToBoolConversion) {
    EXPECT_EQ(TRUE, TypeUtils::TypeUtils::convertToBoolean("true"));
    EXPECT_EQ(TRUE, TypeUtils::TypeUtils::convertToBoolean("TRUE"));
    EXPECT_EQ(TRUE, TypeUtils::TypeUtils::convertToBoolean("True"));
    EXPECT_EQ(TRUE, TypeUtils::TypeUtils::convertToBoolean("TRUe"));

    EXPECT_EQ(FALSE, TypeUtils::TypeUtils::convertToBoolean("false"));
    EXPECT_EQ(FALSE, TypeUtils::TypeUtils::convertToBoolean("FALSE"));
    EXPECT_EQ(FALSE, TypeUtils::TypeUtils::convertToBoolean("False"));
    EXPECT_EQ(FALSE, TypeUtils::TypeUtils::convertToBoolean("fALse"));

    EXPECT_EQ(NULL_BOOL, TypeUtils::TypeUtils::convertToBoolean(""));
}

TEST(TypesTests, StringToBoolConversionErrors) {
    try {
        uint8_t result = TypeUtils::TypeUtils::convertToDouble("TREE");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    try {
        uint8_t result = TypeUtils::TypeUtils::convertToDouble("falst ");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }
}
