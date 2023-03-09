#include <string>

#include "common/exception.h"
#include "common/type_utils.h"
#include "gtest/gtest.h"

using namespace kuzu::common;

TEST(TypesTests, StringToINT64Conversion) {
    EXPECT_EQ(2147483648, TypeUtils::convertStringToNumber<int64_t>("2147483648"));
    EXPECT_EQ(-2147483648, TypeUtils::convertStringToNumber<int64_t>("-2147483648"));
    EXPECT_EQ(2147483648000, TypeUtils::convertStringToNumber<int64_t>("2147483648000"));
    EXPECT_EQ(-2147483648000, TypeUtils::convertStringToNumber<int64_t>("-2147483648000"));
    EXPECT_EQ(648, TypeUtils::convertStringToNumber<int64_t>("648"));
    EXPECT_EQ(-648, TypeUtils::convertStringToNumber<int64_t>("-648"));
    EXPECT_EQ(0, TypeUtils::convertStringToNumber<int64_t>("0"));
    EXPECT_EQ(0, TypeUtils::convertStringToNumber<int64_t>("-0"));
}

TEST(TypesTests, StringToINT64ConversionErrors) {
    // Max overflow
    try {
        TypeUtils::convertStringToNumber<int64_t>("2147483648000000000000");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }

    // Min underflow
    try {
        TypeUtils::convertStringToNumber<int64_t>("-2147483648000000000000");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }

    // Wrong input
    try {
        TypeUtils::convertStringToNumber<int64_t>("qq1244");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }

    // Empty input
    try {
        TypeUtils::convertStringToNumber<int64_t>("");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        TypeUtils::convertStringToNumber<int64_t>("24x[xd432");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        TypeUtils::convertStringToNumber<int64_t>("0L");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }
}

TEST(TypesTests, StringToDoubleConversion) {
    EXPECT_EQ(12235.14, TypeUtils::convertStringToNumber<double_t>("12235.14"));
    EXPECT_EQ(-12235.14013, TypeUtils::convertStringToNumber<double_t>("-12235.14013"));
    EXPECT_EQ(0.001, TypeUtils::convertStringToNumber<double_t>("0.001"));
    EXPECT_EQ(-0.001, TypeUtils::convertStringToNumber<double_t>("-0.001"));
    EXPECT_EQ(0.0, TypeUtils::convertStringToNumber<double_t>("0.0"));
    EXPECT_EQ(0.0, TypeUtils::convertStringToNumber<double_t>("-0.0"));
}

TEST(TypesTests, StringToDoubleConversionErrors) {
    // Wrong input
    try {
        TypeUtils::convertStringToNumber<double_t>("x2.4r432");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }

    // Empty input
    try {
        TypeUtils::convertStringToNumber<double_t>("");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        TypeUtils::convertStringToNumber<double_t>("2.4r432");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }

    // Not all characters consumed
    try {
        TypeUtils::convertStringToNumber<double_t>("0.0f");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }
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
    } catch (std::exception& e) { FAIL(); }

    try {
        TypeUtils::convertToBoolean("falst ");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }

    // empty
    try {
        TypeUtils::convertStringToNumber<double_t>("");
        FAIL();
    } catch (ConversionException& e) {
    } catch (std::exception& e) { FAIL(); }
}
