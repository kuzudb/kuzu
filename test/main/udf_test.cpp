#include "function/string/functions/concat_function.h"
#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

static int32_t add5(int32_t x) {
    return x + 5;
}

TEST_F(ApiTest, UnaryUDFInt64) {
    conn->createScalarFunction("add5", &add5);
    // Dummy query to ensure the add5 function is persistent after a write transaction.
    conn->query("CREATE NODE TABLE PERSON1(ID INT64, PRIMARY KEY(ID))");
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (p:person) return add5(to_int32(p.age))"));
    auto expectedResult = std::vector<std::string>{"40", "35", "50", "25", "25", "30", "45", "88"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(ApiTest, UnaryUDFAddDate) {
    conn->createScalarFunction(
        "addFiveDays", std::vector<LogicalTypeID>{LogicalTypeID::DATE}, LogicalTypeID::DATE, &add5);
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (p:person) return addFiveDays(p.birthdate)"));
    auto expectedResult = std::vector<std::string>{"1900-01-06", "1900-01-06", "1940-06-27",
        "1950-07-28", "1980-10-31", "1980-10-31", "1980-10-31", "1990-12-02"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

static float_t times2(int64_t x) {
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

static double_t timesFloat(int32_t x) {
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

static int16_t strDoubleLen(ku_string_t str) {
    return str.len * 2;
}

TEST_F(ApiTest, UnaryUDFString) {
    conn->createScalarFunction("strDoubleLen", &strDoubleLen);
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (p:person) return strDoubleLen(p.fName)"));
    auto expectedResult = std::vector<std::string>{"10", "6", "10", "6", "18", "12", "8", "98"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

static int64_t addSecondParamTwice(int16_t x, int32_t y) {
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

static int64_t computeStringLenPlus(ku_string_t str, int32_t y) {
    return str.len + y;
}

TEST_F(ApiTest, BinaryUDFStr) {
    conn->createScalarFunction("computeStringLenPlus", &computeStringLenPlus);
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (p:person) return computeStringLenPlus(p.fName, to_int32(p.gender))"));
    auto expectedResult = std::vector<std::string>{"6", "5", "6", "5", "10", "8", "6", "51"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

static int64_t addMicroSeconds(int64_t timestamp, int32_t microSeconds) {
    return timestamp + microSeconds;
}

TEST_F(ApiTest, BinaryAddMicroSeconds) {
    conn->createScalarFunction("addMicro",
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INT32},
        LogicalTypeID::TIMESTAMP, &addMicroSeconds);
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (p:person) return addMicro(p.registerTime, to_int32(p.ID))"));
    auto expectedResult = std::vector<std::string>{"2011-08-20 11:25:30",
        "2008-11-03 15:25:30.000528", "1911-08-20 02:32:21.000003", "2031-11-30 12:25:30.000005",
        "1976-12-23 11:21:42.000007", "1972-07-31 13:22:30.678567", "1976-12-23 04:41:42.000009",
        "2023-02-21 13:25:30.00001"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

static int64_t ternaryAdd(int16_t a, int32_t b, int64_t c) {
    return a + b + c;
}

TEST_F(ApiTest, TernaryUDFInt) {
    conn->createScalarFunction("ternaryAdd", &ternaryAdd);
    auto actualResult = TestHelper::convertResultToString(*conn->query(
        "MATCH (p:person) return ternaryAdd(to_int16(p.gender), to_int32(p.ID), p.age)"));
    auto expectedResult = std::vector<std::string>{"36", "34", "49", "27", "28", "35", "51", "95"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

int32_t ternaryLenTotal(ku_string_t a, blob_t b, ku_string_t c) {
    return (int32_t)(a.len + b.value.len + c.len);
}

TEST_F(ApiTest, TernaryUDFStr) {
    conn->createScalarFunction("ternaryLenTotal",
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::BLOB, LogicalTypeID::STRING},
        LogicalTypeID::INT32, &ternaryLenTotal);
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (m:movies) return ternaryLenTotal(m.name, m.content, m.note)"));
    auto expectedResult = std::vector<std::string>{"64", "68", "84"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

static void validateUDFError(std::function<void()> createFunc, std::string errMsg) {
    try {
        createFunc();
    } catch (CatalogException& e) {
        ASSERT_EQ(std::string(e.what()), errMsg);
        return;
    } catch (Exception&) { FAIL(); }
    FAIL();
}

TEST_F(ApiTest, UDFError) {
    conn->createScalarFunction("add5", &add5);
    validateUDFError([&]() { conn->createScalarFunction("add5", &add5); },
        "Catalog exception: function ADD5 already exists.");
}

TEST_F(ApiTest, UDFTypeError) {
    validateUDFError(
        [&]() {
            conn->createScalarFunction("add5", std::vector<LogicalTypeID>{LogicalTypeID::FLOAT},
                LogicalTypeID::INT32, &add5);
        },
        "Catalog exception: Incompatible udf parameter/return type and templated type.");
}

TEST_F(ApiTest, UnaryUDFMoreParamType) {
    validateUDFError(
        [&]() {
            conn->createScalarFunction("addFiveDays",
                std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT32},
                LogicalTypeID::DATE, &add5);
        },
        "Catalog exception: Expected exactly one parameter type for unary udf. Got: 2.");
}

TEST_F(ApiTest, BinaryUDFMoreParamType) {
    validateUDFError(
        [&]() {
            conn->createScalarFunction("addMicro",
                std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP}, LogicalTypeID::TIMESTAMP,
                &addMicroSeconds);
        },
        "Catalog exception: Expected exactly two parameter types for binary udf. Got: 1.");
}

TEST_F(ApiTest, TernaryUDFMoreParamType) {
    validateUDFError(
        [&]() {
            conn->createScalarFunction("ternaryLenTotal",
                std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::BLOB,
                    LogicalTypeID::STRING, LogicalTypeID::INT32},
                LogicalTypeID::INT32, &ternaryLenTotal);
        },
        "Catalog exception: Expected exactly three parameter types for ternary udf. Got: 4.");
}

static void addFour(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/ = nullptr) {
    KU_ASSERT(parameters.size() == 1);
    auto parameter = parameters[0];
    result.resetAuxiliaryBuffer();
    result.state = parameter->state;
    if (parameter->state->isFlat()) {
        auto pos = parameter->state->selVector->selectedPositions[0];
        result.setValue(pos, parameter->getValue<int64_t>(pos) + 4);
    } else {
        for (auto i = 0u; i < parameter->state->selVector->selectedSize; i++) {
            auto pos = parameter->state->selVector->selectedPositions[i];
            result.setValue(pos, parameter->getValue<int64_t>(pos) + 4);
        }
    }
}

TEST_F(ApiTest, vectorizedUnaryAddFour) {
    conn->createVectorizedFunction<int64_t, int64_t>("addFour", &addFour);
    auto actualResult =
        TestHelper::convertResultToString(*conn->query("MATCH (p:person) return addFour(p.ID)"));
    auto expectedResult = std::vector<std::string>{"4", "6", "7", "9", "11", "12", "13", "14"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

struct AddDate {
    static inline void operation(date_t& left, int64_t& right, date_t& result) {
        result.days = (int32_t)(left.days + right);
    }
};

static void addDate(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/ = nullptr) {
    KU_ASSERT(parameters.size() == 2);
    function::BinaryFunctionExecutor::execute<date_t, int64_t, date_t, AddDate>(
        *parameters[0], *parameters[1], result);
}

TEST_F(ApiTest, vectorizedBinaryAddDate) {
    conn->createVectorizedFunction("addDate",
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64}, LogicalTypeID::DATE,
        &addDate);
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (p:person) return addDate(p.birthdate, p.age)"));
    auto expectedResult = std::vector<std::string>{"1900-01-31", "1900-02-05", "1940-08-06",
        "1950-08-12", "1980-11-15", "1980-11-20", "1980-12-05", "1991-02-18"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

struct ConditionalConcat {
    static inline void operation(
        ku_string_t& a, bool& b, ku_string_t& c, ku_string_t& result, ValueVector& resultVector) {
        // Concat a,c if b is true, otherwise concat c,a.
        if (b) {
            function::Concat::operation(a, c, result, resultVector);
        } else {
            function::Concat::operation(c, a, result, resultVector);
        }
    }
};

static void conditionalConcat(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/ = nullptr) {
    KU_ASSERT(parameters.size() == 3);
    function::TernaryFunctionExecutor::executeString<ku_string_t, bool, ku_string_t, ku_string_t,
        ConditionalConcat>(*parameters[0], *parameters[1], *parameters[2], result);
}

TEST_F(ApiTest, vectorizedTernaryConditionalAdd) {
    conn->createVectorizedFunction<ku_string_t, ku_string_t, bool, ku_string_t>(
        "conditionalConcat", &conditionalConcat);
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (p:person)-[:knows]->(p1:person) return conditionalConcat(p.fName, "
                     "p.isStudent, p1.fName)"));
    auto expectedResult = std::vector<std::string>{"AliceBob", "AliceCarol", "AliceCarol",
        "AliceDan", "AliceDan", "BobAlice", "BobCarol", "BobCarol", "BobDan", "BobDan", "CarolDan",
        "DanCarol", "FarooqElizabeth", "GregElizabeth"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

} // namespace testing
} // namespace kuzu
