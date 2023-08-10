#include "common/string_utils.h"
#include "graph_test/graph_test.h"
#include "processor/plan_mapper.h"
#include "processor/processor.h"

namespace kuzu {
namespace testing {

// TODO(RUI): Refactor this test using our new testing framework once it supports restarting
// database in the middle of test.
class CreateMacroTestTrxTest : public DBTest {

public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST_F(CreateMacroTestTrxTest, createCaseMacro) {
    ASSERT_TRUE(
        conn->query("CREATE MACRO case_macro(x) AS CASE x WHEN 35 THEN x + 1 ELSE x - 5 END")
            ->isSuccess());
    auto query = "match (a:person) return case_macro(a.age) as age";
    auto actualResult = TestHelper::convertResultToString(*conn->query(query));
    auto expectedResult = std::vector<std::string>{"36", "25", "40", "15", "15", "20", "35", "78"};
    sortAndCheckTestResults(actualResult, expectedResult);
    createDBAndConn();
    actualResult = TestHelper::convertResultToString(*conn->query(query));
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(CreateMacroTestTrxTest, createCaseMacroWithoutElse) {
    ASSERT_TRUE(
        conn->query("CREATE MACRO case_macro(x) AS CASE x WHEN 35 THEN x + 1 END")->isSuccess());
    auto query = "match (a:person) return case_macro(a.age) as age";
    auto actualResult = TestHelper::convertResultToString(*conn->query(query));
    auto expectedResult = std::vector<std::string>{"36", "", "", "", "", "", "", ""};
    sortAndCheckTestResults(actualResult, expectedResult);
    createDBAndConn();
    actualResult = TestHelper::convertResultToString(*conn->query(query));
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(CreateMacroTestTrxTest, createFunctionMacro) {
    ASSERT_TRUE(
        conn->query("CREATE MACRO func_macro(x) AS x + 3 + 2.5 + to_float(2.1)")->isSuccess());
    auto query = "match (a:person) return func_macro(a.age) as age";
    auto actualResult = TestHelper::convertResultToString(*conn->query(query));
    auto expectedResult = std::vector<std::string>{"27.600000", "27.600000", "32.600000",
        "37.600000", "42.600000", "47.600000", "52.600000", "90.600000"};
    sortAndCheckTestResults(actualResult, expectedResult);
    createDBAndConn();
    actualResult = TestHelper::convertResultToString(*conn->query(query));
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(CreateMacroTestTrxTest, createLiteralMacro) {
    ASSERT_TRUE(conn->query("CREATE MACRO str_literal() AS 'result'")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE MACRO int_literal() AS to_int64(1) + to_int32(2) + to_int16(3)")
                    ->isSuccess());
    ASSERT_TRUE(conn->query("CREATE MACRO floating_literal() AS 3.5 + to_float(2.1)")->isSuccess());
    ASSERT_TRUE(
        conn->query("CREATE MACRO interval_literal() AS interval('20 minutes')")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE MACRO list_literal() AS [1,3,2]")->isSuccess());
    auto query = "return str_literal(),int_literal(),floating_literal(),interval_literal(),"
                 "list_literal()";
    auto actualResult = TestHelper::convertResultToString(*conn->query(query));
    auto expectedResult = std::vector<std::string>{"result|6|5.600000|00:20:00|[1,3,2]"};
    sortAndCheckTestResults(actualResult, expectedResult);
    createDBAndConn();
    actualResult = TestHelper::convertResultToString(*conn->query(query));
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(CreateMacroTestTrxTest, createPropertyMacro) {
    ASSERT_TRUE(conn->query("CREATE MACRO prop_macro(x) AS x.ID")->isSuccess());
    auto query = "match (a:person) return prop_macro(a) as age";
    auto actualResult = TestHelper::convertResultToString(*conn->query(query));
    auto expectedResult = std::vector<std::string>{"0", "2", "3", "5", "7", "8", "9", "10"};
    sortAndCheckTestResults(actualResult, expectedResult);
    createDBAndConn();
    actualResult = TestHelper::convertResultToString(*conn->query(query));
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(CreateMacroTestTrxTest, createVariableMacro) {
    ASSERT_TRUE(conn->query("CREATE MACRO var_macro(x) AS x")->isSuccess());
    auto query = "match (a:person) return var_macro(a.ID)";
    auto actualResult = TestHelper::convertResultToString(*conn->query(query));
    auto expectedResult = std::vector<std::string>{"0", "2", "3", "5", "7", "8", "9", "10"};
    sortAndCheckTestResults(actualResult, expectedResult);
    createDBAndConn();
    actualResult = TestHelper::convertResultToString(*conn->query(query));
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(CreateMacroTestTrxTest, createMacroReadTrxError) {
    conn->beginReadOnlyTransaction();
    ASSERT_EQ(conn->query("CREATE MACRO var_macro(x) AS x")->getErrorMessage(),
        "Can't execute a write query inside a read-only transaction.");
}

} // namespace testing
} // namespace kuzu
