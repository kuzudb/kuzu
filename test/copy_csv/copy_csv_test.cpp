#include "common/csv_reader/csv_reader.h"
#include "test_helper/test_helper.h"

using namespace std;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::testing;

namespace kuzu {
namespace testing {

class CopyNodeCSVPropertyTest : public InMemoryDBTest {
public:
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-csv-node-property-test/");
    }
};

class CopyCSVSpecialCharTest : public InMemoryDBTest {
public:
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-csv-special-char-test/");
    }
};

class CopyCSVReadLists2BytesPerEdgeTest : public InMemoryDBTest {
public:
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/read-list-tests/2-bytes-per-edge/");
    }
};

class CopyCSVReadLists3BytesPerEdgeTest : public InMemoryDBTest {
public:
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/read-list-tests/3-bytes-per-edge/");
    }
};

class CopyCSVReadLists4BytesPerEdgeTest : public InMemoryDBTest {
public:
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/read-list-tests/4-bytes-per-edge/");
    }
};

class CopyCSVReadLists5BytesPerEdgeTest : public InMemoryDBTest {
public:
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/read-list-tests/5-bytes-per-edge/");
    }
};

class CopyCSVLongStringTest : public InMemoryDBTest {
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-csv-fault-tests/long-string/");
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
    retVal.pNodeTableID = catalog.getReadOnlyVersion()->getNodeTableIDFromName("person");
    retVal.knowsRelTableID = catalog.getReadOnlyVersion()->getRelTableIDFromName("knows");
    retVal.fwdPKnowsLists =
        graph->getRelsStore().getAdjLists(FWD, retVal.pNodeTableID, retVal.knowsRelTableID);
    retVal.bwdPKnowsLists =
        graph->getRelsStore().getAdjLists(BWD, retVal.pNodeTableID, retVal.knowsRelTableID);
    return retVal;
}

ATableAKnowsLists getATableAKnowsLists(const Catalog& catalog, StorageManager* storageManager) {
    ATableAKnowsLists retVal;
    retVal.aNodeTableID = catalog.getReadOnlyVersion()->getNodeTableIDFromName("animal");
    auto knowsRelTableID = catalog.getReadOnlyVersion()->getRelTableIDFromName("knows");
    retVal.fwdAKnowsLists =
        storageManager->getRelsStore().getAdjLists(FWD, retVal.aNodeTableID, knowsRelTableID);
    retVal.bwdAKnowsLists =
        storageManager->getRelsStore().getAdjLists(BWD, retVal.aNodeTableID, knowsRelTableID);
    return retVal;
}
} // namespace testing
} // namespace kuzu

TEST_F(CopyNodeCSVPropertyTest, NodeStructuredStringPropertyTest) {
    auto graph = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("person");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "randomString");
    auto column = reinterpret_cast<StringPropertyColumn*>(
        graph->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID));
    string fName =
        TestHelper::appendKuzuRootPath("dataset/copy-csv-node-property-test/vPerson.csv");
    CSVReaderConfig config;
    CSVReader csvReader(fName, config);
    int lineIdx = 0;
    uint64_t count = 0;
    auto dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    while (csvReader.hasNextLine()) {
        csvReader.hasNextToken();
        csvReader.skipToken();
        csvReader.hasNextToken();
        if ((count % 100) == 0) {
            ASSERT_TRUE(column->isNull(count /* nodeOffset */, dummyReadOnlyTrx.get()));
        } else {
            ASSERT_FALSE(column->isNull(count /* nodeOffset */, dummyReadOnlyTrx.get()));
            EXPECT_EQ(string(csvReader.getString()), column->readValue(lineIdx).strVal);
        }
        lineIdx++;
        csvReader.skipLine();
        count++;
    }
}

void verifyP0ToP5999(KnowsTablePTablePKnowsLists& knowsTablePTablePKnowsLists) {
    // p0 has 5001 fwd edges to p0...p5000
    node_offset_t p0Offset = 0;
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
    for (node_offset_t nodeOffset = 1; nodeOffset <= 5000; ++nodeOffset) {
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
    for (node_offset_t nodeOffset = 5001; nodeOffset < 6000; ++nodeOffset) {
        EXPECT_TRUE(knowsTablePTablePKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(nodeOffset)
                        ->empty());
        EXPECT_TRUE(knowsTablePTablePKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(nodeOffset)
                        ->empty());
    }
}

void verifya0Andp6000(KnowsTablePTablePKnowsLists& knowsTablePTablePKnowsLists,
    const Catalog& catalog, StorageManager* storageManager) {
    auto aTableAKnowsLists = getATableAKnowsLists(catalog, storageManager);
    // a0 has 1 fwd edge to p6000, and no backward edges.
    node_offset_t a0NodeOffset = 0;
    node_offset_t p6000NodeOffset = 6000;
    auto a0FwdList = aTableAKnowsLists.fwdAKnowsLists->readAdjacencyListOfNode(a0NodeOffset);
    EXPECT_EQ(1, a0FwdList->size());
    nodeID_t p6000NodeID(p6000NodeOffset, knowsTablePTablePKnowsLists.pNodeTableID);
    EXPECT_EQ(p6000NodeID, a0FwdList->at(0));
    auto a0BwdList = aTableAKnowsLists.bwdAKnowsLists->readAdjacencyListOfNode(a0NodeOffset);
    EXPECT_TRUE(aTableAKnowsLists.bwdAKnowsLists->readAdjacencyListOfNode(a0NodeOffset)->empty());

    // p6000 has no fwd edges and 1 bwd edge from a0
    EXPECT_TRUE(knowsTablePTablePKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(p6000NodeOffset)
                    ->empty());
    auto p6000BwdList =
        knowsTablePTablePKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(p6000NodeOffset);
    nodeID_t a0NodeID(a0NodeOffset, aTableAKnowsLists.aNodeTableID);
    EXPECT_EQ(1, p6000BwdList->size());
    EXPECT_EQ(a0NodeID, p6000BwdList->at(0));
}

void verifyP6001ToP65999(KnowsTablePTablePKnowsLists& knowsTablePTablePKnowsLists) {
    for (node_offset_t node_offset_t = 6001; node_offset_t < 66000; ++node_offset_t) {
        EXPECT_TRUE(
            knowsTablePTablePKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(node_offset_t)
                ->empty());
        EXPECT_TRUE(
            knowsTablePTablePKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(node_offset_t)
                ->empty());
    }
}

TEST_F(CopyCSVReadLists2BytesPerEdgeTest, ReadLists2BytesPerEdgeTest) {
    auto knowsTablePTablePKnowsLists =
        getKnowsTablePTablePKnowsLists(*getCatalog(*database), getStorageManager(*database));
    verifyP0ToP5999(knowsTablePTablePKnowsLists);
}

TEST_F(CopyCSVReadLists3BytesPerEdgeTest, ReadLists3BytesPerEdgeTest) {
    auto knowsTablePTablePKnowsLists =
        getKnowsTablePTablePKnowsLists(*getCatalog(*database), getStorageManager(*database));
    verifyP0ToP5999(knowsTablePTablePKnowsLists);
    verifya0Andp6000(
        knowsTablePTablePKnowsLists, *getCatalog(*database), getStorageManager(*database));
}

TEST_F(CopyCSVReadLists4BytesPerEdgeTest, ReadLists4BytesPerEdgeTest) {
    auto knowsTablePTablePKnowsLists =
        getKnowsTablePTablePKnowsLists(*getCatalog(*database), getStorageManager(*database));
    verifyP0ToP5999(knowsTablePTablePKnowsLists);
    verifyP6001ToP65999(knowsTablePTablePKnowsLists);
}

TEST_F(CopyCSVReadLists5BytesPerEdgeTest, ReadLists5BytesPerEdgeTest) {
    auto knowsTablePTablePKnowsLists =
        getKnowsTablePTablePKnowsLists(*getCatalog(*database), getStorageManager(*database));
    verifyP0ToP5999(knowsTablePTablePKnowsLists);
    verifya0Andp6000(
        knowsTablePTablePKnowsLists, *getCatalog(*database), getStorageManager(*database));
    verifyP6001ToP65999(knowsTablePTablePKnowsLists);
}

TEST_F(CopyCSVSpecialCharTest, CopySpecialCharsCsv) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("person");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "randomString");
    auto col =
        storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);

    EXPECT_EQ("this is |the first line", col->readValue(0).strVal);
    EXPECT_EQ("the \" should be ignored", col->readValue(1).strVal);
    EXPECT_EQ("the - should be escaped", col->readValue(2).strVal);
    EXPECT_EQ("this -is #a mixed test", col->readValue(3).strVal);
    EXPECT_EQ("only one # should be recognized", col->readValue(4).strVal);
    EXPECT_EQ("this is a ##plain## #string", col->readValue(5).strVal);
    EXPECT_EQ("this is another ##plain## #string with \\", col->readValue(6).strVal);

    tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("organisation");
    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "name");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    EXPECT_EQ("ABFsUni", col->readValue(0).strVal);
    EXPECT_EQ("CsW,ork", col->readValue(1).strVal);
    EXPECT_EQ("DEsW#ork", col->readValue(2).strVal);
}

TEST_F(CopyCSVLongStringTest, LongStringError) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("person");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "fName");
    auto col =
        storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);

    EXPECT_EQ(4096, col->readValue(0).strVal.length());
    string expectedResultName = "Alice";
    auto repeatedTimes = 4096 / expectedResultName.length() + 1;
    ostringstream os;
    for (auto i = 0; i < repeatedTimes; i++) {
        os << expectedResultName;
    }
    EXPECT_EQ(os.str().substr(0, 4096), col->readValue(0).strVal);
    EXPECT_EQ("Bob", col->readValue(1).strVal);

    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "gender");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    EXPECT_EQ(1, col->readValue(0).val.int64Val);
    EXPECT_EQ(2, col->readValue(1).val.int64Val);
}
