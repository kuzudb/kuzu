#include "test/test_utility/include/test_helper.h"

using namespace std;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class CopyCSVArrowNodeTest : public InMemoryDBTest {
    string getInputCSVDir() override { return "dataset/copy-csv-arrow-node-test/"; }
};

TEST_F(CopyCSVArrowNodeTest, ArrowTest) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("arrow");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "feature6");
    auto col =
            storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);

    // Test for random_100k_rows.csv
   EXPECT_EQ(11, col->readValue(54015).val.int64Val);
}