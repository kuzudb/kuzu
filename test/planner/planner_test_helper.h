#include "gtest/gtest.h"
#include "test/mock/mock_catalog.h"

#include "src/binder/include/query_binder.h"
#include "src/parser/include/parser.h"
#include "src/planner/include/planner.h"
#include "src/planner/logical_plan/include/logical_plan_util.h"
#include "src/storage/store/include/nodes_metadata.h"

using ::testing::NiceMock;
using ::testing::Test;

using namespace graphflow::planner;
using namespace graphflow::storage;

class PlannerTest : public Test {

public:
    void SetUp() override {
        catalog.setUp();
        vector<unique_ptr<NodeMetadata>> nodeMetadataPerLabel;
        nodeMetadataPerLabel.push_back(
            make_unique<NodeMetadata>(PERSON_LABEL_ID, NUM_PERSON_NODES));
        nodeMetadataPerLabel.push_back(
            make_unique<NodeMetadata>(ORGANISATION_LABEL_ID, NUM_ORGANISATION_NODES));
        mockNodeMetadata = make_unique<NodesMetadata>(nodeMetadataPerLabel);
    }

    unique_ptr<LogicalPlan> getBestPlan(const string& query) {
        auto statement = Parser::parseQuery(query);
        auto parsedQuery = (RegularQuery*)statement.get();
        auto boundQuery = QueryBinder(catalog).bind(*parsedQuery);
        return Planner::getBestPlan(catalog, *mockNodeMetadata, *boundQuery);
    }

    bool containSubstr(const string& str, const string& substr) {
        return str.find(substr) != string::npos;
    }

private:
    NiceMock<TinySnbCatalog> catalog;
    unique_ptr<NodesMetadata> mockNodeMetadata;
};
