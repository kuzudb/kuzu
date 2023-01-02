#include <string>

#include "common/exception.h"
#include "common/type_utils.h"
#include "gtest/gtest.h"

using namespace kuzu::common;
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
        TypeUtils::convertToInt64("2147483648000000000000");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Min underflow
    try {
        TypeUtils::convertToInt64("-2147483648000000000000");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Wrong input
    try {
        TypeUtils::convertToInt64("qq1244");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Empty input
    try {
        TypeUtils::convertToInt64("");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        TypeUtils::convertToInt64("24x[xd432");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        TypeUtils::convertToInt64("0L");
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
        TypeUtils::convertToDouble("x2.4r432");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Empty input
    try {
        TypeUtils::convertToDouble("");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        TypeUtils::convertToDouble("2.4r432");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        TypeUtils::convertToDouble("0.0f");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }
}

TEST(TypesTests, StringToBoolConversion) {
    EXPECT_EQ(true, TypeUtils::TypeUtils::convertToBoolean("true"));
    EXPECT_EQ(true, TypeUtils::TypeUtils::convertToBoolean("TRUE"));
    EXPECT_EQ(true, TypeUtils::TypeUtils::convertToBoolean("True"));
    EXPECT_EQ(true, TypeUtils::TypeUtils::convertToBoolean("TRUe"));

    EXPECT_EQ(false, TypeUtils::TypeUtils::convertToBoolean("false"));
    EXPECT_EQ(false, TypeUtils::TypeUtils::convertToBoolean("FALSE"));
    EXPECT_EQ(false, TypeUtils::TypeUtils::convertToBoolean("False"));
    EXPECT_EQ(false, TypeUtils::TypeUtils::convertToBoolean("fALse"));
}

TEST(TypesTests, StringToBoolConversionErrors) {
    try {
        TypeUtils::convertToBoolean("TREE");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    try {
        TypeUtils::convertToBoolean("falst ");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }

    // empty
    try {
        TypeUtils::convertToDouble("");
        FAIL();
    } catch (ConversionException& e) {
    } catch (exception& e) { FAIL(); }
}
