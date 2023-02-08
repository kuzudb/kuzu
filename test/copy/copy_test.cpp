#include "common/csv_reader/csv_reader.h"
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

class CopyNodePropertyTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-node-property-test/");
    }
};

class CopySpecialCharTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-special-char-test/");
    }
};

class CopyLargeListTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/read-list-tests/large-list/");
    }
};

class CopyLongStringTest : public DBTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-fault-tests/long-string/");
    }
};

class CopyNodeInitRelTablesTest : public BaseGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
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

TEST_F(CopyNodePropertyTest, NodeStructuredStringPropertyTest) {
    auto graph = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("person");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "randomString");
    auto column = reinterpret_cast<StringPropertyColumn*>(
        graph->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID));
    std::string fName =
        TestHelper::appendKuzuRootPath("dataset/copy-node-property-test/vPerson.csv");
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
            EXPECT_EQ(std::string(csvReader.getString()), column->readValue(lineIdx).strVal);
        }
        lineIdx++;
        csvReader.skipLine();
        count++;
    }
}

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

void verifya0Andp6000(KnowsTablePTablePKnowsLists& knowsTablePTablePKnowsLists,
    const Catalog& catalog, StorageManager* storageManager) {
    auto aTableAKnowsLists = getATableAKnowsLists(catalog, storageManager);
    // a0 has 1 fwd edge to p6000, and no backward edges.
    offset_t a0NodeOffset = 0;
    offset_t p6000NodeOffset = 6000;
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
    for (offset_t node_offset_t = 6001; node_offset_t < 66000; ++node_offset_t) {
        EXPECT_TRUE(
            knowsTablePTablePKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(node_offset_t)
                ->empty());
        EXPECT_TRUE(
            knowsTablePTablePKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(node_offset_t)
                ->empty());
    }
}

TEST_F(CopyLargeListTest, ReadLargeListTest) {
    auto knowsTablePTablePKnowsLists =
        getKnowsTablePTablePKnowsLists(*getCatalog(*database), getStorageManager(*database));
    verifyP0ToP5999(knowsTablePTablePKnowsLists);
}

TEST_F(CopyLargeListTest, AddPropertyWithLargeListTest) {
    ASSERT_TRUE(conn->query("alter table knows add length INT64 DEFAULT 50")->isSuccess());
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("match (:person)-[e:knows]->(:person) return e.length"));
    std::vector<std::string> expectedResult{10001, "50"};
    ASSERT_EQ(actualResult, expectedResult);
    ASSERT_TRUE(conn->query("match (:person)-[e:knows]->(:person) set e.length = 37")->isSuccess());
    actualResult = TestHelper::convertResultToString(
        *conn->query("match (p:person)-[e:knows]->(:person) return e.length"), true);
    expectedResult = std::vector<std::string>{10001, "37"};
    ASSERT_EQ(actualResult, expectedResult);
}

TEST_F(CopySpecialCharTest, CopySpecialChars) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("person");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "randomString");
    auto col =
        storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);

    EXPECT_EQ("this is |the first line", col->readValue(0).strVal);
    EXPECT_EQ("the \" should be ignored", col->readValue(1).strVal);
    EXPECT_EQ("the - should be escaped", col->readValue(2).strVal);
    EXPECT_EQ("this -is #a mixed test", col->readValue(3).strVal);
    EXPECT_EQ("only one # should be recognized", col->readValue(4).strVal);
    EXPECT_EQ("this is a #plain# string", col->readValue(5).strVal);
    EXPECT_EQ("this is another #plain# string with \\", col->readValue(6).strVal);

    tableID = catalog->getReadOnlyVersion()->getTableID("organisation");
    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "name");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    EXPECT_EQ("ABFsUni", col->readValue(0).strVal);
    EXPECT_EQ("CsW,ork", col->readValue(1).strVal);
    EXPECT_EQ("DEsW#ork", col->readValue(2).strVal);
}

TEST_F(CopyLongStringTest, LongStringError) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("person");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "fName");
    auto col =
        storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);

    EXPECT_EQ(4096, col->readValue(0).strVal.length());
    std::string expectedResultName = "Alice";
    auto repeatedTimes = 4096 / expectedResultName.length() + 1;
    std::ostringstream os;
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

TEST_F(CopyNodeInitRelTablesTest, CopyNodeAndQueryEmptyRelTable) {
    // The purpose of this test is to make sure that we correctly initialize the rel table for the
    // newly added nodes (E.g. we need to set the column entry to NULL and create listHeader for
    // each newly added node).
    ASSERT_TRUE(
        conn->query(
                "create node table person (ID INt64, fName StRING, gender INT64, isStudent "
                "BoOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, "
                "registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames "
                "STRING[], courseScoresPerTerm INT64[][], PRIMARY KEY (ID));")
            ->isSuccess());
    ASSERT_TRUE(conn->query("create rel table knows (FROM person TO person, date DATE, meetTime "
                            "TIMESTAMP, validInterval INTERVAL, comments STRING[], MANY_MANY);")
                    ->isSuccess());
    ASSERT_TRUE(conn->query("create rel table meets (FROM person TO person, MANY_ONE);"));
    ASSERT_TRUE(
        conn->query("COPY person FROM \"" +
                    TestHelper::appendKuzuRootPath("dataset/tinysnb/vPerson.csv\" (HEADER=true);"))
            ->isSuccess());
    ASSERT_TRUE(conn->query("MATCH (:person)-[:knows]->(:person) return count(*)")->isSuccess());
}
