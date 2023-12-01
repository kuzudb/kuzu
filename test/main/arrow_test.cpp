#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::testing;

class ArrowTest : public ApiTest {};

TEST_F(ArrowTest, getArrowResult) {
    auto query = "MATCH (a:person) WHERE a.fName = 'Bob' RETURN a.fName";
    auto result = conn->query(query);
    auto arrowArray = result->getNextArrowChunk(1);
    ASSERT_EQ(arrowArray->length, 1);
    ASSERT_EQ(arrowArray->null_count, 0);
    ASSERT_EQ(arrowArray->n_children, 1);
    // FIXME: Not sure where the length of the string is stored
    ASSERT_EQ(std::string((const char*)arrowArray->children[0]->buffers[2], 3), "Bob");
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
