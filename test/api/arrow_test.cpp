#include "api_test/api_test.h"
#include "common/exception/runtime.h"

using namespace kuzu::common;
using namespace kuzu::testing;

class ArrowTest : public ApiTest {};

TEST_F(ArrowTest, resultToArrow) {
    auto query = "MATCH (a:person) WHERE a.fName = 'Bob' RETURN a.fName";
    auto result = conn->query(query);
    auto arrowArray = result->getNextArrowChunk(1);
    ASSERT_EQ(arrowArray->length, 1);
    ASSERT_EQ(arrowArray->null_count, 0);
    ASSERT_EQ(arrowArray->n_children, 1);
    // FIXME: Not sure where the length of the string is stored
    ASSERT_EQ(std::string((const char*)arrowArray->children[0]->buffers[2], 3), "Bob");
    ASSERT_FALSE(result->hasNextArrowChunk());
    arrowArray->release(arrowArray.get());
}

TEST_F(ArrowTest, queryAsArrow) {
    auto query = "MATCH (a:person) WHERE a.fName = 'Bob' RETURN a.fName";
    auto result = conn->queryAsArrow(query, 1);
    auto arrowArray = result->getNextArrowChunk(1);
    ASSERT_EQ(arrowArray->length, 1);
    ASSERT_EQ(arrowArray->null_count, 0);
    ASSERT_EQ(arrowArray->n_children, 1);
    // FIXME: Not sure where the length of the string is stored
    ASSERT_EQ(std::string((const char*)arrowArray->children[0]->buffers[2], 3), "Bob");
    ASSERT_FALSE(result->hasNextArrowChunk());
    arrowArray->release(arrowArray.get());
}

TEST_F(ArrowTest, getArrowResult) {
    auto query = "MATCH (a:person) WHERE a.fName = 'Bob' RETURN a.fName";
    auto result = conn->queryAsArrow(query, 1);
    try {
        result->getNextArrowChunk(2);
        FAIL();
    } catch (const Exception& e) {
        ASSERT_STREQ(e.what(), "Runtime exception: Chunk size does not match expected value 1.");
    }
    try {
        (void)result->hasNext();
        FAIL();
    } catch (const Exception& e) {
        ASSERT_STREQ(e.what(),
            "ArrowQueryResult does not implement hasNext. Use MaterializedQueryResult instead.");
    }
    try {
        (void)result->getNext();
        FAIL();
    } catch (const Exception& e) {
        ASSERT_STREQ(e.what(),
            "ArrowQueryResult does not implement getNext. Use MaterializedQueryResult instead.");
    }
    try {
        (void)result->toString();
        FAIL();
    } catch (const Exception& e) {
        ASSERT_STREQ(e.what(),
            "ArrowQueryResult does not implement toString. Use MaterializedQueryResult instead.");
    }
    ASSERT_TRUE(result->hasNextArrowChunk());
    auto arrowArray = result->getNextArrowChunk(1);
    ASSERT_EQ(arrowArray->length, 1);
    ASSERT_EQ(arrowArray->null_count, 0);
    ASSERT_FALSE(result->hasNextArrowChunk());
    arrowArray->release(arrowArray.get());
}

TEST_F(ArrowTest, getArrowSchema) {
    auto query = "MATCH (a:person) RETURN a.fName as NAME";
    auto result = conn->query(query);
    auto schema = result->getArrowSchema();
    ASSERT_EQ(schema->n_children, 1);
    ASSERT_EQ(std::string(schema->children[0]->name), "NAME");
    schema->release(schema.get());
}
