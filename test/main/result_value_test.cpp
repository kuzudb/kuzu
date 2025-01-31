#include "common/exception/runtime.h"
#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
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
    } catch (Exception& exception) {
        FAIL();
    } catch (std::exception& exception) {
        FAIL();
    }
}

TEST_F(ResultValueTest, getResultValueException) {
    auto query = "MATCH (a:person) RETURN a.fName";
    auto result = conn->query(query);
    auto flatTuple = result->getNext();
    try {
        flatTuple->getValue(100);
        FAIL();
    } catch (RuntimeException& exception) {
        ASSERT_STREQ("Runtime exception: ValIdx is out of range. Number of values in flatTuple: 1, "
                     "valIdx: 100.",
            exception.what());
    } catch (Exception& exception) {
        FAIL();
    } catch (std::exception& exception) {
        FAIL();
    }
}

static decltype(auto) copyFlatTuple(kuzu::processor::FlatTuple* tuple) {
    std::vector<std::unique_ptr<kuzu::common::Value>> ret;
    for (uint32_t i = 0; i < tuple->len(); i++) {
        ret.emplace_back(tuple->getValue(i)->copy());
    }
    return ret;
}

TEST_F(ResultValueTest, getNextExample) {
    std::unique_ptr<kuzu::main::QueryResult> result = conn->query("MATCH (p:person) RETURN p.*");
    std::vector<std::vector<std::unique_ptr<kuzu::common::Value>>> tuples;
    while (result->hasNext()) {
        auto tuple = result->getNext();
        tuples.emplace_back(copyFlatTuple(tuple.get()));
    }

    for (const auto& tuple : tuples) {
        for (const auto& value : tuple) {
            ASSERT_FALSE(value->isNull());
        }
    }
}
