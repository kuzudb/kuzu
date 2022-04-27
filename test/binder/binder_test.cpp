#include "gtest/gtest.h"
#include "test/mock/mock_catalog.h"

#include "src/binder/include/query_binder.h"
#include "src/common/include/configs.h"
#include "src/parser/include/parser.h"

using namespace graphflow::parser;
using namespace graphflow::binder;

using ::testing::NiceMock;
using ::testing::Test;

class BinderTest : public Test {

public:
    void SetUp() override { catalog.setUp(); }

protected:
    NiceMock<TinySnbCatalog> catalog;
};

TEST_F(BinderTest, VarLenExtendMaxDepthTest) {
    // If the upper bound of the varLenEtend is larger than VAR_LENGTH_EXTEND_MAX_DEPTH, the
    // upper bound will be set to VAR_LENGTH_EXTEND_MAX_DEPTH.
    auto input = "MATCH (a:person)-[:knows*2..32]->(b:person) return count(*)";
    auto boundRegularQuery = QueryBinder(catalog).bind(*Parser::parseQuery(input));
    auto queryRel =
        boundRegularQuery->getSingleQuery(0)->getQueryPart(0)->getQueryGraph(0)->getQueryRel(0);
    ASSERT_EQ(queryRel->getLowerBound(), 2);
    ASSERT_EQ(queryRel->getUpperBound(), VAR_LENGTH_EXTEND_MAX_DEPTH);
}
