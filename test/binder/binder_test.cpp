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

TEST_F(BinderTest, VarLenExtendMaxDepthTest) {
    // If the upper bound of the varLenEtend is larger than VAR_LENGTH_EXTEND_MAX_DEPTH, the
    // upper bound will be set to VAR_LENGTH_EXTEND_MAX_DEPTH.
    auto input = "MATCH (a:person)-[:knows*2..32]->(b:person) return count(*)";
    auto boundRegularQuery =
        Binder(catalog).bind(*reinterpret_cast<RegularQuery*>(Parser::parseQuery(input).get()));
    auto queryRel = ((BoundRegularQuery*)boundRegularQuery.get())
                        ->getSingleQuery(0)
                        ->getQueryPart(0)
                        ->getQueryGraph(0)
                        ->getQueryRel(0);
    ASSERT_EQ(queryRel->getLowerBound(), 2);
    ASSERT_EQ(queryRel->getUpperBound(), VAR_LENGTH_EXTEND_MAX_DEPTH);
}

// TODO(Ziyi): remove these tests once we implement the backend for load_csv.
TEST_F(BinderTest, CopyCSVWithParsingOptionsTest) {
    auto input =
        R"(COPY person FROM "person_0_0.csv" (ESCAPE="\\", DELIM=';', QUOTE='"',LIST_BEGIN='{',LIST_END='}');)";
    auto boundStatement = Binder(catalog).bind(*Parser::parseQuery(input));
    auto boundCopyCSV = (BoundCopyCSV*)boundStatement.get();
    ASSERT_EQ(boundCopyCSV->getCSVFileName(), "person_0_0.csv");
    ASSERT_EQ(boundCopyCSV->getLabelID(), 0);
    ASSERT_EQ(boundCopyCSV->getIsNodeLabel(), true);
    ASSERT_EQ(boundCopyCSV->getParsingOptions()["ESCAPE"], '\\');
    ASSERT_EQ(boundCopyCSV->getParsingOptions()["DELIM"], ';');
    ASSERT_EQ(boundCopyCSV->getParsingOptions()["QUOTE"], '"');
    ASSERT_EQ(boundCopyCSV->getParsingOptions()["LIST_BEGIN"], '{');
    ASSERT_EQ(boundCopyCSV->getParsingOptions()["LIST_END"], '}');
}

TEST_F(BinderTest, CopyCSVWithoutParsingOptionsTest) {
    auto input = R"(COPY person FROM "person_0_0.csv";)";
    auto boundStatement = Binder(catalog).bind(*Parser::parseQuery(input));
    auto boundCopyCSV = (BoundCopyCSV*)boundStatement.get();
    ASSERT_EQ(boundCopyCSV->getCSVFileName(), "person_0_0.csv");
    ASSERT_EQ(boundCopyCSV->getLabelID(), 0);
    ASSERT_EQ(boundCopyCSV->getIsNodeLabel(), true);
    ASSERT_EQ(boundCopyCSV->getParsingOptions()["ESCAPE"], '\\');
    ASSERT_EQ(boundCopyCSV->getParsingOptions()["DELIM"], ',');
    ASSERT_EQ(boundCopyCSV->getParsingOptions()["QUOTE"], '\"');
    ASSERT_EQ(boundCopyCSV->getParsingOptions()["LIST_BEGIN"], '[');
    ASSERT_EQ(boundCopyCSV->getParsingOptions()["LIST_END"], ']');
}
