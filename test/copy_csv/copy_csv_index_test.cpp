#include "test/test_utility/include/test_helper.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class TinySnbIndexTest : public InMemoryDBTest {
    string getInputCSVDir() override { return "dataset/tinysnb/"; }
};

TEST_F(TinySnbIndexTest, PersonNodeIDIndex) {
    auto storageManager = database->getStorageManager();
    auto& nodesStore = storageManager->getNodesStore();
    auto personIDIndex = nodesStore.getIDIndex(0);
    int64_t personIDs[8] = {0, 2, 3, 5, 7, 8, 9, 10};
    node_offset_t nodeOffset;
    auto dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    for (auto i = 0u; i < 8; i++) {
        auto found = personIDIndex->lookup(dummyReadOnlyTrx.get(), personIDs[i], nodeOffset);
        ASSERT_TRUE(found);
        ASSERT_EQ(nodeOffset, i);
    }
}

TEST_F(TinySnbIndexTest, OrganisationNodeIDIndex) {
    auto storageManager = database->getStorageManager();
    auto& nodesStore = storageManager->getNodesStore();
    auto organisationIDIndex = nodesStore.getIDIndex(1);
    int64_t organisationIDs[3] = {1, 4, 6};
    node_offset_t nodeOffset;
    auto dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    for (auto i = 0u; i < 3; i++) {
        auto found =
            organisationIDIndex->lookup(dummyReadOnlyTrx.get(), organisationIDs[i], nodeOffset);
        ASSERT_TRUE(found);
        ASSERT_EQ(nodeOffset, i);
    }
}
