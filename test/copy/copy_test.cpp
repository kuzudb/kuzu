#include <iostream>
#include <string>

#include "common/string_utils.h"
#include "graph_test/graph_test.h"
#include "json.hpp"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::testing;
using namespace kuzu::transaction;

namespace kuzu {
namespace testing {

class CopyLargeListTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/read-list-tests/large-list/");
    }
};

struct KnowsTablePTablePKnowsLists {
    table_id_t knowsRelTableID;
    table_id_t pNodeTableID;
    AdjLists* fwdPKnowsLists;
    AdjLists* bwdPKnowsLists;
};

struct ATableAKnowsLists {
    table_id_t aNodeTableID;
    AdjLists* fwdAKnowsLists;
    AdjLists* bwdAKnowsLists;
};

KnowsTablePTablePKnowsLists getKnowsTablePTablePKnowsLists(
    const Catalog& catalog, StorageManager* graph) {
    KnowsTablePTablePKnowsLists retVal;
    retVal.pNodeTableID = catalog.getReadOnlyVersion()->getTableID("person");
    retVal.knowsRelTableID = catalog.getReadOnlyVersion()->getTableID("knows");
    retVal.fwdPKnowsLists = graph->getRelsStore().getAdjLists(FWD, retVal.knowsRelTableID);
    retVal.bwdPKnowsLists = graph->getRelsStore().getAdjLists(BWD, retVal.knowsRelTableID);
    return retVal;
}

ATableAKnowsLists getATableAKnowsLists(const Catalog& catalog, StorageManager* storageManager) {
    ATableAKnowsLists retVal;
    retVal.aNodeTableID = catalog.getReadOnlyVersion()->getTableID("animal");
    auto knowsRelTableID = catalog.getReadOnlyVersion()->getTableID("knows");
    retVal.fwdAKnowsLists = storageManager->getRelsStore().getAdjLists(FWD, knowsRelTableID);
    retVal.bwdAKnowsLists = storageManager->getRelsStore().getAdjLists(BWD, knowsRelTableID);
    return retVal;
}
} // namespace testing
} // namespace kuzu

void verifyP0ToP5999(KnowsTablePTablePKnowsLists& knowsTablePTablePKnowsLists) {
    // p0 has 5001 fwd edges to p0...p5000
    offset_t p0Offset = 0;
    auto pOFwdList = knowsTablePTablePKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(p0Offset);
    EXPECT_EQ(5001, pOFwdList->size());
    for (int nodeOffset = 0; nodeOffset <= 5000; ++nodeOffset) {
        nodeID_t nodeIDPi(nodeOffset, knowsTablePTablePKnowsLists.pNodeTableID);
        auto nbrNodeID = pOFwdList->at(nodeOffset);
        EXPECT_EQ(nodeIDPi, nbrNodeID);
    }
    // p0 has only 1 bwd edge, which from itself
    auto p0BwdList = knowsTablePTablePKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(p0Offset);
    EXPECT_EQ(1, p0BwdList->size());
    nodeID_t p0NodeID(p0Offset, knowsTablePTablePKnowsLists.pNodeTableID);
    EXPECT_EQ(p0NodeID, pOFwdList->at(0));

    // p1,p2,...,p5000 have a single fwd edge to p5000 and 1 bwd edge from node p0
    nodeID_t nodeIDP5000(5000ul, knowsTablePTablePKnowsLists.pNodeTableID);
    for (offset_t nodeOffset = 1; nodeOffset <= 5000; ++nodeOffset) {
        auto fwdAdjList =
            knowsTablePTablePKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(nodeOffset);
        EXPECT_EQ(1, fwdAdjList->size());
        EXPECT_EQ(nodeIDP5000, fwdAdjList->at(0));
        auto bwdAdjList =
            knowsTablePTablePKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(nodeOffset);
        EXPECT_EQ(1, fwdAdjList->size());
        EXPECT_EQ(p0NodeID, bwdAdjList->at(0));
    }

    // p5001 to p6000 are singletons
    for (offset_t nodeOffset = 5001; nodeOffset < 6000; ++nodeOffset) {
        EXPECT_TRUE(knowsTablePTablePKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(nodeOffset)
                        ->empty());
        EXPECT_TRUE(knowsTablePTablePKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(nodeOffset)
                        ->empty());
    }
}

TEST_F(CopyLargeListTest, ReadLargeListTest) {
    auto knowsTablePTablePKnowsLists =
        getKnowsTablePTablePKnowsLists(*getCatalog(*database), getStorageManager(*database));
    verifyP0ToP5999(knowsTablePTablePKnowsLists);
}

// TEST_F(CopyLargeListTest, AddPropertyWithLargeListTest) {
//    ASSERT_TRUE(conn->query("alter table knows add length INT64 DEFAULT 50")->isSuccess());
//    auto actualResult = TestHelper::convertResultToString(
//        *conn->query("match (:person)-[e:knows]->(:person) return e.length"));
//    std::vector<std::string> expectedResult{10001, "50"};
//    ASSERT_EQ(actualResult, expectedResult);
//    ASSERT_TRUE(conn->query("match (:person)-[e:knows]->(:person) set e.length
//    =37")->isSuccess()); actualResult = TestHelper::convertResultToString(
//        *conn->query("match (p:person)-[e:knows]->(:person) return e.length"), true);
//    expectedResult = std::vector<std::string>{10001, "37"};
//    ASSERT_EQ(actualResult, expectedResult);
//}
