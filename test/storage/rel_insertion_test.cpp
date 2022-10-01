#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class RelInsertionTest : public DBTest {

public:
    void SetUp() override {
        bufferManager = make_unique<BufferManager>();
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
        DBTest::SetUp();
        initWithoutLoadingGraph();
        relIDProperty = make_shared<ValueVector>(INT64, memoryManager.get());
        relIDValues = (int64_t*)relIDProperty->values;
        lengthProperty = make_shared<ValueVector>(INT64, memoryManager.get());
        lengthValues = (int64_t*)lengthProperty->values;
        placeProperty = make_shared<ValueVector>(STRING, memoryManager.get());
        placeValues = (gf_string_t*)placeProperty->values;
        srcNodeVector = make_shared<ValueVector>(NODE_ID, memoryManager.get());
        dstNodeVector = make_shared<ValueVector>(NODE_ID, memoryManager.get());
        dataChunk->insert(0, relIDProperty);
        dataChunk->insert(1, lengthProperty);
        dataChunk->insert(2, placeProperty);
        dataChunk->insert(3, srcNodeVector);
        dataChunk->insert(4, dstNodeVector);
        dataChunk->state->currIdx = 0;
    }

    string getInputCSVDir() override { return "dataset/rel-insertion-tests/"; }

    void initWithoutLoadingGraph() { createDBAndConn(); }

    void insertRels(table_id_t srcTableID, table_id_t dstTableID, uint64_t numValuesToInsert = 100,
        bool insertNullValues = false, bool testLongString = false) {
        auto placeStr = gf_string_t();
        if (testLongString) {
            placeStr.overflowPtr =
                reinterpret_cast<uint64_t>(placeProperty->getOverflowBuffer().allocateSpace(100));
        }
        for (auto i = 0u; i < numValuesToInsert; i++) {
            relIDValues[0] = 1051 + i;
            lengthValues[0] = i;
            placeStr.set((testLongString ? "long long string prefix " : "") + to_string(i));
            placeValues[0] = placeStr;
            if (insertNullValues) {
                lengthProperty->setNull(0, i % 2);
                placeProperty->setNull(0, true /* isNull */);
            }
            ((nodeID_t*)srcNodeVector->values)[0] = nodeID_t(1, srcTableID);
            ((nodeID_t*)dstNodeVector->values)[0] = nodeID_t(i + 1, dstTableID);
            database->getStorageManager()
                ->getRelsStore()
                .getRel(0 /* relTableID */)
                ->getRelUpdateStore()
                ->addRel(vector<shared_ptr<ValueVector>>{
                    srcNodeVector, dstNodeVector, lengthProperty, placeProperty, relIDProperty});
        }
    }

    static void validateInsertedEdgesLengthProperty(
        QueryResult* queryResult, uint64_t numValuesToCheck, bool containNullValues = false) {
        for (auto i = 0u; i < numValuesToCheck; i++) {
            auto tuple = queryResult->getNext();
            ASSERT_EQ(tuple->isNull(0), containNullValues && i % 2);
            if (!containNullValues || i % 2 == 0) {
                ASSERT_EQ(tuple->getValue(0)->val.int64Val, i);
            }
        }
    }

    void incorrectVectorErrorTest(
        vector<shared_ptr<ValueVector>> srcDstNodeIDAndRelProperties, string errorMsg) {
        try {
            database->getStorageManager()
                ->getRelsStore()
                .getRel(0 /* relTableID */)
                ->getRelUpdateStore()
                ->addRel(srcDstNodeIDAndRelProperties);
            FAIL();
        } catch (InternalException& exception) {
            ASSERT_EQ(exception.what(), errorMsg);
        } catch (Exception& e) { FAIL(); }
    }

public:
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    shared_ptr<ValueVector> relIDProperty;
    int64_t* relIDValues;
    shared_ptr<ValueVector> lengthProperty;
    int64_t* lengthValues;
    shared_ptr<ValueVector> placeProperty;
    gf_string_t* placeValues;
    shared_ptr<ValueVector> srcNodeVector;
    shared_ptr<ValueVector> dstNodeVector;
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(5 /* numValueVectors */);
};

TEST_F(RelInsertionTest, InsertRelsToEmptyList) {
    insertRels(0 /* srcTableID */, 0 /* dstTableID */);
    conn->beginWriteTransaction();
    auto result = conn->query("match (:animal)-[e:knows]->(:animal) return e.length");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 100);
    validateInsertedEdgesLengthProperty(result.get(), 100 /* numValuesToCheck */);
    auto conn1 = make_unique<Connection>(database.get());
    auto readResult = conn1->query("match (:animal)-[e:knows]->(:animal) return e.length");
    ASSERT_EQ(readResult->getNumTuples(), 0);
}

TEST_F(RelInsertionTest, InsertRelsToLargeList) {
    auto numValuesToInsert = 1;
    insertRels(1 /* srcTableID */, 1 /* dstTableID */, numValuesToInsert /* numValuesToInsert */);
    conn->beginWriteTransaction();
    auto result = conn->query("match (:person)-[e:knows]->(:person) return e.place");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 2500 + numValuesToInsert);
    for (auto i = 0u; i < 2500; i++) {
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->val.strVal.getAsString(), to_string(3000 - i));
    }
    for (auto i = 0u; i < numValuesToInsert; i++) {
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->val.strVal.getAsString(), to_string(i));
    }
}

TEST_F(RelInsertionTest, InsertRelsToSmallList) {
    insertRels(0 /* srcTableID */, 1 /* dstTableID */);
    conn->beginWriteTransaction();
    auto result = conn->query("match (:animal)-[e:knows]->(:person) return e.length");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 151);
    for (auto i = 0u; i < 51; i++) {
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->val.int64Val, i);
        if (i == 1) {
            validateInsertedEdgesLengthProperty(result.get(), 100 /* numValuesToCheck */);
        }
    }
}

TEST_F(RelInsertionTest, InsertPartialNullRels) {
    auto numValuesToInsert = 1000;
    insertRels(
        0 /* srcTableID */, 1 /* dstTableID */, numValuesToInsert, true /* insertNullValues */);
    conn->beginWriteTransaction();
    auto result = conn->query("match (:animal)-[e:knows]->(:person) return e.length");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 51 + numValuesToInsert);
    for (auto i = 0u; i < 51; i++) {
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->val.int64Val, i);
        if (i == 1) {
            validateInsertedEdgesLengthProperty(
                result.get(), numValuesToInsert, true /* containNullValues */);
        }
    }
}

TEST_F(RelInsertionTest, InsertAllNullRels) {
    auto numValuesToInsert = 1000;
    insertRels(
        0 /* srcTableID */, 1 /* dstTableID */, numValuesToInsert, true /* insertNullValues */);
    conn->beginWriteTransaction();
    auto result = conn->query("match (:animal)-[e:knows]->(:person) return e.place");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 51 + numValuesToInsert);
    for (auto i = 0u; i < 51; i++) {
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->val.strVal.getAsString(), to_string(1000 - i));
        if (i == 1) {
            for (auto j = 0u; j < numValuesToInsert; j++) {
                auto tuple = result->getNext();
                ASSERT_EQ(tuple->isNull(0), true);
            }
        }
    }
}

TEST_F(RelInsertionTest, InsertRelsAndQueryBWDList) {
    insertRels(1 /* srcTableID */, 0 /* dstTableID */);
    conn->beginWriteTransaction();
    // This query will read the backward adjacency list.
    auto result = conn->query("match (:person)-[e:knows]->(:animal) return e.length");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 100);
    validateInsertedEdgesLengthProperty(result.get(), 100 /* numValuesToCheck */);
}

TEST_F(RelInsertionTest, InsertLongStringsToLargeList) {
    auto numValuesToInsert = 500;
    insertRels(1 /* srcTableID */, 1 /* dstTableID */, numValuesToInsert /* numValuesToInsert */,
        false /* containNullValues */, true /* testLongString */);
    conn->beginWriteTransaction();
    auto result = conn->query("match (:person)-[e:knows]->(:person) return e.place");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 2500 + numValuesToInsert);
    for (auto i = 0u; i < 2500; i++) {
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->val.strVal.getAsString(), to_string(3000 - i));
    }
    for (auto i = 0u; i < numValuesToInsert; i++) {
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->val.strVal.getAsString(),
            "long long string prefix " + to_string(i));
    }
}

TEST_F(RelInsertionTest, InCorrectVectorErrorTest) {
    incorrectVectorErrorTest(vector<shared_ptr<ValueVector>>{lengthProperty, placeProperty,
                                 relIDProperty, srcNodeVector, relIDProperty, dstNodeVector},
        "Expected number of valueVectors: 5. Given: 6.");
    incorrectVectorErrorTest(vector<shared_ptr<ValueVector>>{srcNodeVector, dstNodeVector,
                                 placeProperty, lengthProperty, relIDProperty},
        "Expected vector with type INT64, Given: STRING.");
    incorrectVectorErrorTest(vector<shared_ptr<ValueVector>>{srcNodeVector, relIDProperty,
                                 lengthProperty, placeProperty, relIDProperty},
        "The first two vectors of srcDstNodeIDAndRelProperties should be src/dstNodeVector.");
    ((nodeID_t*)dstNodeVector->values)[0].tableID = 5;
    incorrectVectorErrorTest(vector<shared_ptr<ValueVector>>{srcNodeVector, dstNodeVector,
                                 lengthProperty, placeProperty, relIDProperty},
        "ListUpdateStore for tableID: 5 doesn't exist.");
}
