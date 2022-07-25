#include "test/test_utility/include/test_helper.h"

#include "src/common/include/overflow_buffer_utils.h"
#include "src/storage/include/wal_replayer.h"

using namespace graphflow::storage;
using namespace graphflow::testing;

// Note: ID and nodeOffsetForPropKeys in this test are equal for each node, so we use nodeID and
// nodeOffsetForPropKeys interchangeably.
class UnstructuredPropertyListsUpdateTests : public BaseGraphLoadingTest {

public:
    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        initWithoutLoadingGraph();
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        overflowBuffer = make_unique<OverflowBuffer>(database->getMemoryManager());
        readConn = make_unique<Connection>(database.get());
        personNodeLabel =
            database->getCatalog()->getReadOnlyVersion()->getNodeLabelFromName("person");
        personNodeTable = database->getStorageManager()->getNodesStore().getNode(personNodeLabel);

        existingStrVal.dataType = DataType(DataTypeID::STRING);
        string existingStr = "abcdefghijklmn";
        OverflowBufferUtils::copyString(
            existingStr.c_str(), existingStr.size(), existingStrVal.val.strVal, *overflowBuffer);
        existingIntVal.dataType = DataType(DataTypeID::INT64);
        existingIntVal.val.int64Val = 123456;

        shortStrVal.dataType = DataType(DataTypeID::STRING);
        string shortStr = "short";
        OverflowBufferUtils::copyString(
            shortStr.c_str(), shortStr.size(), shortStrVal.val.strVal, *overflowBuffer);
        longStrVal.dataType = DataType(DataTypeID::STRING);
        string longStr = "new-long-string";
        OverflowBufferUtils::copyString(
            longStr.c_str(), longStr.size(), longStrVal.val.strVal, *overflowBuffer);
        intVal.dataType = DataType(DataTypeID::INT64);
        intVal.val.int64Val = 677121;

        conn->beginWriteTransaction();
    }

    string getInputCSVDir() override {
        return "dataset/unstructured-property-lists-updates-tests/";
    }

    // TODO(Xiyang): Currently we are manually calling set in getUnstrPropertyLists. Once we have
    // frontend support, these sets should also be done through Cypher queries.
    void setPropertyOfNode123(string strPropKey, Value& newStrVal) {
        uint32_t unstrPropKey = database->getCatalog()
                                    ->getReadOnlyVersion()
                                    ->getNodeProperty(personNodeLabel, strPropKey)
                                    .propertyID;
        personNodeTable->getUnstrPropertyLists()->setProperty(123, unstrPropKey, &newStrVal);
    }

    void removeProperty(node_offset_t nodeOffset, string propKey) {
        uint32_t unstrPropKey = database->getCatalog()
                                    ->getReadOnlyVersion()
                                    ->getNodeProperty(personNodeLabel, propKey)
                                    .propertyID;
        personNodeTable->getUnstrPropertyLists()->removeProperty(nodeOffset, unstrPropKey);
    }

    void queryAndVerifyResults(node_offset_t nodeOffset, string intPropKey, string strPropKey,
        Value& expectedIntForWriteTrx, Value& expectedStrValueForWriteTrx,
        Value& expectedIntForReadTrx, Value& expectedStrValueForReadTrx) {
        string query = "MATCH (a:person) WHERE a.ID = " + to_string(nodeOffset) + " RETURN a." +
                       intPropKey + ", a." + strPropKey;
        auto writeConTuple = conn->query(query)->getNext();
        ASSERT_FALSE(writeConTuple->isNull(0));
        ASSERT_EQ(writeConTuple->getValue(0)->val.int64Val, expectedIntForWriteTrx.val.int64Val);
        ASSERT_FALSE(writeConTuple->isNull(1));
        ASSERT_EQ(writeConTuple->getValue(1)->val.strVal, expectedStrValueForWriteTrx.val.strVal);

        auto readConTuple = readConn->query(query)->getNext();
        ASSERT_FALSE(readConTuple->isNull(0));
        ASSERT_EQ(readConTuple->getValue(0)->val.int64Val, expectedIntForReadTrx.val.int64Val);
        ASSERT_FALSE(readConTuple->isNull(1));
        ASSERT_EQ(readConTuple->getValue(1)->val.strVal, expectedStrValueForReadTrx.val.strVal);
    }

public:
    unique_ptr<Connection> readConn;
    NodeTable* personNodeTable;
    // Overflow is needed to update unstructured properties with long strings
    unique_ptr<OverflowBuffer> overflowBuffer;
    label_t personNodeLabel;
    Value existingStrVal, existingIntVal, shortStrVal, longStrVal, intVal;
};

TEST_F(UnstructuredPropertyListsUpdateTests, UpdateExistingFixedLenPropertiesUseShortString) {
    setPropertyOfNode123("ui123", intVal);
    setPropertyOfNode123("us123", shortStrVal);
    queryAndVerifyResults(123, "ui123", "us123", intVal /* expected int for write trx */,
        shortStrVal /* expected str for write trx */,
        existingIntVal /* expected int for read trx */,
        existingStrVal /* expected str for write trx */);
}

TEST_F(UnstructuredPropertyListsUpdateTests, UpdateExistingFixedLenAndStringPropTest) {
    setPropertyOfNode123("us123", longStrVal);
    queryAndVerifyResults(123, "ui123", "us123", existingIntVal /* expected int for write trx */,
        longStrVal /* expected str for write trx */, existingIntVal /* expected int for read trx */,
        existingStrVal /* expected str for write trx */);
}

TEST_F(UnstructuredPropertyListsUpdateTests, InsertNonExistingPropsTest) {
    setPropertyOfNode123("us125", longStrVal);
    setPropertyOfNode123("ui124", intVal);
    string query = "MATCH (a:person) WHERE a.ID = 123 RETURN a.ui124, a.us125";
    auto writeConTuple = conn->query(query)->getNext();
    ASSERT_EQ(writeConTuple->getValue(0)->val.int64Val, intVal.val.int64Val);
    ASSERT_EQ(writeConTuple->getValue(1)->val.strVal, longStrVal.val.strVal);

    auto readConTuple = readConn->query(query)->getNext();
    ASSERT_TRUE(readConTuple->isNull(0));
    ASSERT_TRUE(readConTuple->isNull(1));
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveExistingProperties) {
    removeProperty(123, "us123");
    removeProperty(123, "ui123");
    string query = "MATCH (a:person) WHERE a.ID = 123 RETURN a.ui123, a.us123";
    auto writeConTuple = conn->query(query)->getNext();
    ASSERT_TRUE(writeConTuple->isNull(0));
    ASSERT_TRUE(writeConTuple->isNull(1));

    auto readConTuple = readConn->query(query)->getNext();
    ASSERT_EQ(readConTuple->getValue(0)->val.int64Val, existingIntVal.val.int64Val);
    ASSERT_EQ(readConTuple->getValue(1)->val.strVal, existingStrVal.val.strVal);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveNonExistingProperties) {
    removeProperty(123, "us125");
    removeProperty(123, "ui124");
    string query = "MATCH (a:person) WHERE a.ID = 123 RETURN a.ui123, a.us123";
    queryAndVerifyResults(123, "ui123", "us123", existingIntVal /* expected int for write trx */,
        existingStrVal /* expected str for write trx */,
        existingIntVal /* expected int for read trx */,
        existingStrVal /* expected str for write trx */);
    // TODO(Semih): When commit & checkpointing logic is implemented, we can test here that the wal
    // is empty because this is one case where there is actually not updates.
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveNewlyAddedProperties) {
    setPropertyOfNode123("us125", longStrVal);
    removeProperty(123, "us125");
    setPropertyOfNode123("ui123", intVal);
    removeProperty(123, "ui123");

    string query = "MATCH (a:person) WHERE a.ID = 123 RETURN a.ui123, a.us123, a.us125";
    auto writeConTuple = conn->query(query)->getNext();
    ASSERT_TRUE(writeConTuple->isNull(0));
    ASSERT_EQ(writeConTuple->getValue(1)->val.strVal, existingStrVal.val.strVal);
    ASSERT_TRUE(writeConTuple->isNull(2));

    auto readConTuple = readConn->query(query)->getNext();
    ASSERT_EQ(readConTuple->getValue(0)->val.int64Val, existingIntVal.val.int64Val);
    ASSERT_EQ(readConTuple->getValue(1)->val.strVal, existingStrVal.val.strVal);
    ASSERT_TRUE(readConTuple->isNull(2));
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAllPropertiesBySetPropertyListEmpty) {
    personNodeTable->getUnstrPropertyLists()->setPropertyListEmpty(123);
    string query = "MATCH (a:person) WHERE a.ID = 123 RETURN a.ui123, a.us123";
    auto writeConTuple = conn->query(query)->getNext();
    ASSERT_TRUE(writeConTuple->isNull(0));
    ASSERT_TRUE(writeConTuple->isNull(1));

    auto readConTuple = readConn->query(query)->getNext();
    ASSERT_EQ(readConTuple->getValue(0)->val.int64Val, existingIntVal.val.int64Val);
    ASSERT_EQ(readConTuple->getValue(1)->val.strVal, existingStrVal.val.strVal);
}

TEST_F(UnstructuredPropertyListsUpdateTests, InsertALargeNumberOfProperties) {
    for (uint64_t nodeOffsetForPropKeys = 0; nodeOffsetForPropKeys <= 600;
         ++nodeOffsetForPropKeys) {
        setPropertyOfNode123("ui" + to_string(nodeOffsetForPropKeys), intVal);
        setPropertyOfNode123("us" + to_string(nodeOffsetForPropKeys), longStrVal);
    }
    for (uint64_t nodeOffsetForPropKeys = 0; nodeOffsetForPropKeys <= 600;
         ++nodeOffsetForPropKeys) {
        string query = "MATCH (a:person) WHERE a.ID = 123 RETURN a.ui" +
                       to_string(nodeOffsetForPropKeys) + ", a.us" +
                       to_string(nodeOffsetForPropKeys);
        auto writeConTuple = conn->query(query)->getNext();
        ASSERT_EQ(writeConTuple->getValue(0)->val.int64Val, intVal.val.int64Val);
        ASSERT_EQ(writeConTuple->getValue(1)->val.strVal, longStrVal.val.strVal);

        auto readConTuple = readConn->query(query)->getNext();
        if (nodeOffsetForPropKeys == 123) {
            ASSERT_EQ(readConTuple->getValue(0)->val.int64Val, existingIntVal.val.int64Val);
            ASSERT_EQ(readConTuple->getValue(1)->val.strVal, existingStrVal.val.strVal);
        } else {
            ASSERT_TRUE(readConTuple->isNull(0));
            ASSERT_TRUE(readConTuple->isNull(1));
        }
    }
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAllUnstructuredPropertiesOfAllNodes) {
    // We blindly remove all unstructured properties from all nodes one by one
    for (uint64_t queryNodeOffset = 0; queryNodeOffset <= 600; queryNodeOffset++) {
        for (uint64_t nodeOffsetForPropKeys = 0; nodeOffsetForPropKeys <= 600;
             ++nodeOffsetForPropKeys) {
            removeProperty(queryNodeOffset, "ui" + to_string(nodeOffsetForPropKeys));
            removeProperty(queryNodeOffset, "us" + to_string(nodeOffsetForPropKeys));
        }
    }

    uint64_t numQueries = 0;
    for (uint64_t nodeOffsetForPropKeys = 0; nodeOffsetForPropKeys <= 600;
         nodeOffsetForPropKeys++) {
        string query = "MATCH (a:person) WHERE a.ui" + to_string(nodeOffsetForPropKeys) +
                       " IS NOT NULL OR a.us" + to_string(nodeOffsetForPropKeys) +
                       " IS NOT NULL RETURN count (*)";
        auto writeConTuple = conn->query(query)->getNext();
        ASSERT_EQ(writeConTuple->getValue(0)->val.int64Val, 0);

        auto readConTuple = readConn->query(query)->getNext();
        if (nodeOffsetForPropKeys == 250) {
            // If we are looking for 250's properties, e.g., ui250 and us250, then we expect the
            // count to be only 1 because only node 250 contains these
            ASSERT_EQ(readConTuple->getValue(0)->val.int64Val, 1);
        } else {
            // If we are not looking for 250's properties, e.g., ui0 and us0, then we expect the
            // count to be 2 because there are 2 nodes with those properties: 1 is the node with
            // nodeID=nodeOffsetForPropKeys; and 2 is node 250 which has all unstructured
            // properties.
            ASSERT_EQ(readConTuple->getValue(0)->val.int64Val, 2);
        }
    }
}
