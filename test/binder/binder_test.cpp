#include "gtest/gtest.h"
#include "test/mock/mock_catalog.h"

#include "src/binder/include/create_node_clause_binder.h"
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
    auto boundRegularQuery = QueryBinder(catalog).bind(
        *reinterpret_cast<RegularQuery*>(Parser::parseQuery(input).get()));
    auto queryRel =
        boundRegularQuery->getSingleQuery(0)->getQueryPart(0)->getQueryGraph(0)->getQueryRel(0);
    ASSERT_EQ(queryRel->getLowerBound(), 2);
    ASSERT_EQ(queryRel->getUpperBound(), VAR_LENGTH_EXTEND_MAX_DEPTH);
}

TEST_F(BinderTest, CreateNodeWithPKTest) {
    auto input = "CREATE NODE TABLE PERSON(NAME STRING, AGE INT64, BIRTHDATE DATE, PRIMARY "
                 "KEY (NAME))";
    auto parsedExpression = Parser::parseQuery(input);
    auto boundCreateNodeClause = CreateNodeClauseBinder(&catalog).bind(
        *reinterpret_cast<CreateNodeClause*>(parsedExpression.get()));
    catalog.addNodeLabel(*boundCreateNodeClause);
    ASSERT_EQ(catalog.getNumNodeLabels(), 1);
    auto nodeLabel = catalog.getNodeLabelFromName("PERSON");
    auto properties = catalog.getAllNodeProperties(nodeLabel);
    ASSERT_EQ(properties[0].name, "NAME");
    ASSERT_EQ(properties[0].dataType, DataType(STRING));
    ASSERT_EQ(properties[1].name, "AGE");
    ASSERT_EQ(properties[1].dataType, DataType(INT64));
    ASSERT_EQ(properties[2].name, "BIRTHDATE");
    ASSERT_EQ(properties[2].dataType, DataType(DATE));
    auto pkProperty = catalog.getNodePrimaryKeyProperty(nodeLabel);
    ASSERT_EQ(pkProperty.name, "NAME");
    ASSERT_EQ(pkProperty.dataType, DataType(STRING));
}

TEST_F(BinderTest, CreateNodeWithWithoutPKTest) {
    auto input = "CREATE NODE TABLE BOY(AGE INT64, HOBBY STRING, HEIGHT DOUBLE)";
    auto parsedExpression = Parser::parseQuery(input);
    auto boundCreateNodeClause = CreateNodeClauseBinder(&catalog).bind(
        *reinterpret_cast<CreateNodeClause*>(parsedExpression.get()));
    catalog.addNodeLabel(*boundCreateNodeClause);
    ASSERT_EQ(catalog.getNumNodeLabels(), 1);
    auto nodeLabel = catalog.getNodeLabelFromName("BOY");
    auto properties = catalog.getAllNodeProperties(nodeLabel);
    ASSERT_EQ(properties[0].name, "AGE");
    ASSERT_EQ(properties[0].dataType, DataType(INT64));
    ASSERT_EQ(properties[1].name, "HOBBY");
    ASSERT_EQ(properties[1].dataType, DataType(STRING));
    ASSERT_EQ(properties[2].name, "HEIGHT");
    ASSERT_EQ(properties[2].dataType, DataType(DOUBLE));
    auto pkProperty = catalog.getNodePrimaryKeyProperty(nodeLabel);
    ASSERT_EQ(pkProperty.name, "AGE");
    ASSERT_EQ(pkProperty.dataType, DataType(INT64));
}
