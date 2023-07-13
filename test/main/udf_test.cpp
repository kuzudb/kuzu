#include "main_test_helper/main_test_helper.h"

namespace kuzu {
namespace testing {

int64_t add5(int64_t x) {
    return x + 5;
}

TEST_F(ApiTest, UnaryUDFInt64) {
    conn->createScalarFunction("add5", &add5);
    auto actualResult =
        TestHelper::convertResultToString(*conn->query("MATCH (p:person) return add5(p.age)"));
    auto expectedResult = std::vector<std::string>{"40", "35", "50", "25", "25", "30", "45", "88"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

float_t times2(int64_t x) {
    return float_t(2 * x);
}

TEST_F(ApiTest, UnaryUDFFloat) {
    conn->createScalarFunction("times2", &times2);
    auto actualResult =
        TestHelper::convertResultToString(*conn->query("MATCH (p:person) return times2(p.age)"));
    auto expectedResult = std::vector<std::string>{"70.000000", "60.000000", "90.000000",
        "40.000000", "40.000000", "50.000000", "80.000000", "166.000000"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

double_t timesFloat(int32_t x) {
    return (double_t)2.4 * x;
}

TEST_F(ApiTest, UnaryUDFDouble) {
    conn->createScalarFunction("timesFloat", &timesFloat);
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (p:person) return timesFloat(to_int32(p.ID))"));
    auto expectedResult = std::vector<std::string>{"0.000000", "4.800000", "7.200000", "12.000000",
        "16.800000", "19.200000", "21.600000", "24.000000"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

int16_t strDoubleLen(common::ku_string_t str) {
    return str.len * 2;
}

TEST_F(ApiTest, UnaryUDFString) {
    conn->createScalarFunction("strDoubleLen", &strDoubleLen);
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (p:person) return strDoubleLen(p.fName)"));
    auto expectedResult = std::vector<std::string>{"10", "6", "10", "6", "18", "12", "8", "98"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

int64_t addSecondParamTwice(int16_t x, int32_t y) {
    return x + y + y;
}

TEST_F(ApiTest, BinaryUDFFlatUnflat) {
    conn->createScalarFunction("addSecondParamTwice", &addSecondParamTwice);
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (p:person)-[:knows]->(p1:person) return "
                     "addSecondParamTwice(to_int16(p.ID), to_int32(p1.age))"));
    auto expectedResult = std::vector<std::string>{
        "60", "90", "40", "72", "92", "42", "73", "63", "43", "75", "65", "95", "57", "87"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

int64_t computeStringLenPlus(common::ku_string_t str, int32_t y) {
    return str.len + y;
}

TEST_F(ApiTest, BinaryUDFStr) {
    conn->createScalarFunction("computeStringLenPlus", &computeStringLenPlus);
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (p:person) return computeStringLenPlus(p.fName, to_int32(p.gender))"));
    auto expectedResult = std::vector<std::string>{"6", "5", "6", "5", "10", "8", "6", "51"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

int64_t ternaryAdd(int16_t a, int32_t b, int64_t c) {
    return a + b + c;
}

TEST_F(ApiTest, TernaryUDFInt) {
    conn->createScalarFunction("ternaryAdd", &ternaryAdd);
    auto actualResult = TestHelper::convertResultToString(*conn->query(
        "MATCH (p:person) return ternaryAdd(to_int16(p.gender), to_int32(p.ID), p.age)"));
    auto expectedResult = std::vector<std::string>{"36", "34", "49", "27", "28", "35", "51", "95"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(ApiTest, UDFError) {
    conn->createScalarFunction("add5", &add5);
    try {
        conn->createScalarFunction("add5", &add5);
    } catch (common::CatalogException& e) {
        ASSERT_EQ(std::string(e.what()), "Catalog exception: function ADD5 already exists.");
        return;
    } catch (common::Exception&) { FAIL(); }
    FAIL();
}

} // namespace testing
} // namespace kuzu
