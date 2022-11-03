#include "gtest/gtest.h"
#include "test/mock/mock_catalog.h"

#include "src/binder/include/binder.h"
#include "src/common/include/configs.h"
#include "src/parser/include/parser.h"

using namespace graphflow::parser;
using namespace graphflow::binder;

using ::testing::Test;

class BinderTest : public Test {

public:
    void SetUp() override { catalog.setUp(); }

protected:
    NiceMock<TinySnbCatalog> catalog;
};

TEST_F(BinderTest, QueryPartRewriteTest) {
    auto input = "MATCH (a:person) MATCH (b:person) MATCH (c:person) RETURN COUNT(*)";
    auto boundStatement =
        Binder(catalog).bind(*reinterpret_cast<RegularQuery*>(Parser::parseQuery(input).get()));
    auto queryPart = ((BoundRegularQuery*)boundStatement.get())->getSingleQuery(0)->getQueryPart(0);
    ASSERT_EQ(queryPart->getNumReadingClause(), 1);
}

TEST_F(BinderTest, QueryPartRewriteTest2) {
    auto input = "MATCH (a:person) OPTIONAL MATCH (b:person) MATCH (c:person) RETURN COUNT(*)";
    auto boundStatement =
        Binder(catalog).bind(*reinterpret_cast<RegularQuery*>(Parser::parseQuery(input).get()));
    auto queryPart = ((BoundRegularQuery*)boundStatement.get())->getSingleQuery(0)->getQueryPart(0);
    ASSERT_EQ(queryPart->getNumReadingClause(), 3);
}

TEST_F(BinderTest, VarLenExtendMaxDepthTest) {
    // If the upper bound of the varLenEtend is larger than VAR_LENGTH_EXTEND_MAX_DEPTH, the
    // upper bound will be set to VAR_LENGTH_EXTEND_MAX_DEPTH.
    auto input = "MATCH (a:person)-[:knows*2..32]->(b:person) return count(*)";
    auto boundStatement =
        Binder(catalog).bind(*reinterpret_cast<RegularQuery*>(Parser::parseQuery(input).get()));
    auto normalizedQueryPart =
        ((BoundRegularQuery*)boundStatement.get())->getSingleQuery(0)->getQueryPart(0);
    for (auto i = 0u; i < normalizedQueryPart->getNumReadingClause(); i++) {
        ASSERT_EQ(normalizedQueryPart->getReadingClause(i)->getClauseType(), ClauseType::MATCH);
        auto boundMatchClause = (BoundMatchClause*)normalizedQueryPart->getReadingClause(i);
        auto queryRel =
            boundMatchClause->getQueryGraphCollection()->getQueryGraph(0)->getQueryRel(0);
        ASSERT_EQ(queryRel->getLowerBound(), 2);
        ASSERT_EQ(queryRel->getUpperBound(), VAR_LENGTH_EXTEND_MAX_DEPTH);
    }
}
