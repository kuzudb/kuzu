#include "binder/binder.h"
#include "binder/visitor/confidential_statement_analyzer.h"
#include "graph_test/private_graph_test.h"
#include "parser/parser.h"

namespace kuzu {
namespace testing {

TEST_F(EmptyDBTest, ConfidentialStatementVisitor) {
    createDBAndConn();
    conn->query("BEGIN TRANSACTION");
    database->addExtensionOption("binder_test_conf", common::LogicalTypeID::UINT64,
        common::Value{5}, true /* isConfidential */);
    database->addExtensionOption("binder_test_not_conf", common::LogicalTypeID::UINT64,
        common::Value{2}, false /* isConfidential */);
    binder::Binder binder{conn->getClientContext()};

    binder::ConfidentialStatementAnalyzer analyzerConfidential{};
    auto parsed = parser::Parser::parseQuery("CALL binder_test_conf = 3");
    auto boundStatement = binder.bind(*parsed[0]);
    analyzerConfidential.visit(*boundStatement);
    ASSERT_TRUE(analyzerConfidential.isConfidential());

    binder::ConfidentialStatementAnalyzer analyzerNotConfidential{};
    parsed = parser::Parser::parseQuery("CALL binder_test_not_conf = 444");
    boundStatement = binder.bind(*parsed[0]);
    analyzerConfidential.visit(*boundStatement);
    ASSERT_FALSE(analyzerConfidential.isConfidential());

    parsed = parser::Parser::parseQuery("CALL timeout = 4");
    boundStatement = binder.bind(*parsed[0]);
    analyzerNotConfidential.visit(*boundStatement);
    ASSERT_FALSE(analyzerNotConfidential.isConfidential());
}

} // namespace testing
} // namespace kuzu
