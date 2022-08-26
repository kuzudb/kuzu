#include "gtest/gtest.h"
#include "test/mock/mock_catalog.h"

#include "src/binder/include/binder.h"
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
        vector<unique_ptr<NodeMetadata>> nodeMetadataPerTable;
        nodeMetadataPerTable.push_back(
            make_unique<NodeMetadata>(PERSON_TABLE_ID, NUM_PERSON_NODES));
        nodeMetadataPerTable.push_back(
            make_unique<NodeMetadata>(ORGANISATION_TABLE_ID, NUM_ORGANISATION_NODES));
        mockNodeMetadata = make_unique<NodesMetadata>(nodeMetadataPerTable);
    }

    unique_ptr<LogicalPlan> getBestPlan(const string& query) {
        auto statement = Parser::parseQuery(query);
        auto parsedQuery = (RegularQuery*)statement.get();
        auto boundQuery = Binder(catalog).bind(*parsedQuery);
        return Planner::getBestPlan(catalog, *mockNodeMetadata, *boundQuery);
    }

    bool containSubstr(const string& str, const string& substr) {
        return str.find(substr) != string::npos;
    }

private:
    NiceMock<TinySnbCatalog> catalog;
    unique_ptr<NodesMetadata> mockNodeMetadata;
};
