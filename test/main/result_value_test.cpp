#include "main_test_helper/main_test_helper.h"

using namespace kuzu::testing;

class ResultValueTest : public ApiTest {};

TEST_F(ResultValueTest, getNextException) {
    auto query = "MATCH (o:organisation) RETURN o.name";
    auto result = conn->query(query);
    try {
        for (int i = 0; i < 4; i++) {
            result->getNext();
        }
        FAIL();
    } catch (RuntimeException& exception) {
        ASSERT_STREQ("Runtime exception: No more tuples in QueryResult, Please check hasNext() "
                     "before calling getNext().",
            exception.what());
    } catch (Exception& exception) { FAIL(); } catch (std::exception& exception) {
        FAIL();
    }
}

TEST_F(ResultValueTest, getResultValueException) {
    auto query = "MATCH (a:person) RETURN a.fName";
    auto result = conn->query(query);
    auto flatTuple = result->getNext();
    try {
        flatTuple->getResultValue(100);
        FAIL();
    } catch (RuntimeException& exception) {
        ASSERT_STREQ("Runtime exception: ValIdx is out of range. Number of values in flatTuple: 1, "
                     "valIdx: 100.",
            exception.what());
    } catch (Exception& exception) { FAIL(); } catch (std::exception& exception) {
        FAIL();
    }
}

TEST_F(ResultValueTest, getResultValueWrongTypeException) {
    auto query = "MATCH (a:person) RETURN a.fName";
    auto result = conn->query(query);
    auto flatTuple = result->getNext();
    try {
        flatTuple->getResultValue(0)->getBooleanVal();
        FAIL();
    } catch (RuntimeException& exception) {
        ASSERT_STREQ("Runtime exception: Cannot get BOOL value from the STRING result value.",
            exception.what());
    } catch (Exception& exception) { FAIL(); } catch (std::exception& exception) {
        FAIL();
    }
}
