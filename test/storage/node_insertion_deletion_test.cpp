#include "graph_test/graph_test.h"
#include "storage/storage_manager.h"
#include "storage/wal_replayer.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

// Note: ID and nodeOffset in this test are equal for each node, so we use nodeID and nodeOffset
// interchangeably.
class NodeInsertionDeletionTests : public DBTest {

public:
    void SetUp() override {
        DBTest::SetUp();
        initDBAndConnection();
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/node-insertion-deletion-tests/int64-pk/");
    }

    void initDBAndConnection() {
        createDBAndConn();
        readConn = std::make_unique<Connection>(database.get());
        table_id_t personTableID =
            getCatalog(*database)->getReadOnlyVersion()->getTableID("person");
        personNodeTable = getStorageManager(*database)->getNodesStore().getNodeTable(personTableID);
        uint32_t idPropertyID = getCatalog(*database)
                                    ->getReadOnlyVersion()
                                    ->getNodeProperty(personTableID, "ID")
                                    .propertyID;
        idColumn = getStorageManager(*database)->getNodesStore().getNodePropertyColumn(
            personTableID, idPropertyID);
        conn->beginWriteTransaction();
    }

    offset_t addNode() {
        // TODO(Guodong/Semih/Xiyang): Currently it is not clear when and from where the hash index,
        // structured columns, adjacency Lists, and adj columns of a
        // newly added node should be informed that a new node is being inserted, so these data
        // structures either write values or NULLs or empty Lists etc. Within the scope of these
        // tests we only have an ID column and we are manually from outside
        // NodesStatisticsAndDeletedIDs adding a NULL value for the ID. This should change later.
        offset_t nodeOffset = personNodeTable->getNodeStatisticsAndDeletedIDs()->addNode(
            personNodeTable->getTableID());
        auto dataChunk = std::make_shared<DataChunk>(2);
        // Flatten the data chunk
        dataChunk->state->currIdx = 0;
        auto nodeIDVector =
            std::make_shared<ValueVector>(LogicalTypeID::INTERNAL_ID, getMemoryManager(*database));
        dataChunk->insert(0, nodeIDVector);
        auto idVector =
            std::make_shared<ValueVector>(LogicalTypeID::INT64, getMemoryManager(*database));
        dataChunk->insert(1, idVector);
        ((nodeID_t*)nodeIDVector->getData())[0].offset = nodeOffset;
        idVector->setNull(0, true /* is null */);
        idColumn->write(nodeIDVector.get(), idVector.get());
        return nodeOffset;
    }

public:
    std::unique_ptr<Connection> readConn;
    NodeTable* personNodeTable;
    Column* idColumn;
};

TEST_F(NodeInsertionDeletionTests, DeletingSameNodeOffsetErrorsTest) {
    personNodeTable->getNodeStatisticsAndDeletedIDs()->deleteNode(
        personNodeTable->getTableID(), 3 /* person w/ offset/ID 3 */);
    try {
        personNodeTable->getNodeStatisticsAndDeletedIDs()->deleteNode(personNodeTable->getTableID(),
            3 /* person w/ offset/ID 3 again, which should error  */);
        FAIL();
    } catch (RuntimeException& e) {
    } catch (Exception& e) { FAIL(); }
}

TEST_F(NodeInsertionDeletionTests, DeleteAddMixedTest) {
    for (offset_t nodeOffset = 1000; nodeOffset < 9000; ++nodeOffset) {
        personNodeTable->getNodeStatisticsAndDeletedIDs()->deleteNode(
            personNodeTable->getTableID(), nodeOffset);
    }
    for (int i = 0; i < 8000; ++i) {
        auto nodeOffset = addNode();
        ASSERT_TRUE(nodeOffset >= 1000 && nodeOffset < 9000);
    }

    // Add two additional node offsets
    for (int i = 0; i < 10; ++i) {
        auto nodeOffset = addNode();
        ASSERT_EQ(nodeOffset, 10000 + i);
    }

    std::string query = "MATCH (a:person) RETURN count(*)";
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10010);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10000);
    conn->commit();
    conn->beginWriteTransaction();
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10010);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10010);

    for (offset_t nodeOffset = 0; nodeOffset < 10010; ++nodeOffset) {
        personNodeTable->getNodeStatisticsAndDeletedIDs()->deleteNode(
            personNodeTable->getTableID(), nodeOffset);
    }

    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 0);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10010);
    conn->commit();
    conn->beginWriteTransaction();
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 0);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 0);

    for (int i = 0; i < 5000; ++i) {
        auto nodeOffset = addNode();
        ASSERT_TRUE(nodeOffset >= 0 && nodeOffset < 10010);
    }

    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 5000);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 0);
    conn->commit();
    conn->beginWriteTransaction();
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 5000);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 5000);
}
