#include "gtest/gtest.h"
#include "test/mock/mock_graph.h"

#include "src/binder/include/query_binder.h"
#include "src/parser/include/parser.h"
#include "src/planner/include/planner.h"

using ::testing::NiceMock;
using ::testing::Test;

using namespace graphflow::planner;

class PlannerTest : public Test {

public:
    void SetUp() override { graph.setUp(); }

    unique_ptr<LogicalPlan> getBestPlan(const string& query) {
        auto parsedQuery = Parser::parseQuery(query);
        auto boundQuery = QueryBinder(graph.getCatalog()).bind(*parsedQuery);
        return Planner::getBestPlan(graph, *boundQuery);
    }

    vector<unique_ptr<LogicalPlan>> getAllPlans(const string& query) {
        auto parsedQuery = Parser::parseQuery(query);
        auto boundQuery = QueryBinder(graph.getCatalog()).bind(*parsedQuery);
        return Planner::getAllPlans(graph, *boundQuery);
    }

    bool containSubstr(const string& str, const string& substr) {
        return str.find(substr) != string::npos;
    }

private:
    NiceMock<TinySnbGraph> graph;
};
