#include "test/planner/planner_test_helper.h"
#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class RelInsertionTest : public DBTest {

public:
    void SetUp() override {
        bufferManager = make_unique<BufferManager>();
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
        DBTest::SetUp();
        relIDPropertyVector = make_shared<ValueVector>(INT64, memoryManager.get());
        relIDValues = (int64_t*)relIDPropertyVector->values;
        lengthPropertyVector = make_shared<ValueVector>(INT64, memoryManager.get());
        lengthValues = (int64_t*)lengthPropertyVector->values;
        placePropertyVector = make_shared<ValueVector>(STRING, memoryManager.get());
        placeValues = (gf_string_t*)placePropertyVector->values;
        srcNodeVector = make_shared<ValueVector>(NODE_ID, memoryManager.get());
        dstNodeVector = make_shared<ValueVector>(NODE_ID, memoryManager.get());
        tagPropertyVector = make_shared<ValueVector>(
            DataType{LIST, make_unique<DataType>(STRING)}, memoryManager.get());
        tagValues = (gf_list_t*)tagPropertyVector->values;
        dataChunk->insert(0, relIDPropertyVector);
        dataChunk->insert(1, lengthPropertyVector);
        dataChunk->insert(2, placePropertyVector);
        dataChunk->insert(3, tagPropertyVector);
        dataChunk->insert(4, srcNodeVector);
        dataChunk->insert(5, dstNodeVector);
        dataChunk->state->currIdx = 0;
        vectorsToInsertToKnows = vector<shared_ptr<ValueVector>>{srcNodeVector, dstNodeVector,
            lengthPropertyVector, placePropertyVector, tagPropertyVector, relIDPropertyVector};
        vectorsToInsertToPlays = vector<shared_ptr<ValueVector>>{
            srcNodeVector, dstNodeVector, placePropertyVector, relIDPropertyVector};
        vectorsToInsertToHasOwner = vector<shared_ptr<ValueVector>>{srcNodeVector, dstNodeVector,
            lengthPropertyVector, placePropertyVector, relIDPropertyVector};
        vectorsToInsertToTeaches = vector<shared_ptr<ValueVector>>{
            srcNodeVector, dstNodeVector, lengthPropertyVector, relIDPropertyVector};
    }

    void commitOrRollbackConnectionAndInitDBIfNecessary(
        bool isCommit, TransactionTestType transactionTestType) {
        commitOrRollbackConnection(isCommit, transactionTestType);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            createDBAndConn();
        }
    }

    string getStringValToValidate(uint64_t val) {
        string result = to_string(val);
        if (val % 2 == 0) {
            for (auto i = 0; i < 2; i++) {
                result.append(to_string(val));
            }
        }
        return result;
    }

    string getInputCSVDir() override { return "dataset/rel-insertion-tests/"; }

    inline void insertOneRelToRelTable(
        table_id_t relTableID, vector<shared_ptr<ValueVector>> vectorsToInsert) {
        database->getStorageManager()
            ->getRelsStore()
            .getRelTable(relTableID)
            ->insertRels(vectorsToInsert);
    }

    inline void insertOneKnowsRel() {
        insertOneRelToRelTable(0 /* relTableID */, vectorsToInsertToKnows);
    }

    inline void insertOnePlaysRel() {
        insertOneRelToRelTable(1 /* relTableID */, vectorsToInsertToPlays);
    }

    inline void insertOneHasOwnerRel() {
        insertOneRelToRelTable(2 /* relTableID */, vectorsToInsertToHasOwner);
    }

    inline void insertOneTeachesRel() {
        insertOneRelToRelTable(3 /* relTableID */, vectorsToInsertToTeaches);
    }

    void insertRelsToKnowsTable(table_id_t srcTableID, table_id_t dstTableID,
        uint64_t numValuesToInsert = 100, bool insertNullValues = false,
        bool testLongString = false) {
        auto placeStr = gf_string_t();
        if (testLongString) {
            placeStr.overflowPtr = reinterpret_cast<uint64_t>(
                placePropertyVector->getOverflowBuffer().allocateSpace(100));
        }
        auto tagList = gf_list_t();
        tagList.overflowPtr =
            reinterpret_cast<uint64_t>(tagPropertyVector->getOverflowBuffer().allocateSpace(100));
        tagList.size = 1;
        for (auto i = 0u; i < numValuesToInsert; i++) {
            relIDValues[0] = 1051 + i;
            lengthValues[0] = i;
            placeStr.set((testLongString ? "long long string prefix " : "") + to_string(i));
            placeValues[0] = placeStr;
            tagList.set((uint8_t*)&placeStr, DataType(LIST, make_unique<DataType>(STRING)));
            tagValues[0] = tagList;
            if (insertNullValues) {
                lengthPropertyVector->setNull(0, i % 2);
                placePropertyVector->setNull(0, true /* isNull */);
            }
            ((nodeID_t*)srcNodeVector->values)[0] = nodeID_t(1, srcTableID);
            ((nodeID_t*)dstNodeVector->values)[0] = nodeID_t(i + 1, dstTableID);
            insertOneKnowsRel();
        }
    }

    void incorrectVectorErrorTest(
        vector<shared_ptr<ValueVector>> srcDstNodeIDAndRelProperties, string errorMsg) {
        try {
            database->getStorageManager()
                ->getRelsStore()
                .getRelTable(0 /* relTableID */)
                ->getAdjAndPropertyListsUpdateStore()
                ->insertRelIfNecessary(srcDstNodeIDAndRelProperties);
            FAIL();
        } catch (InternalException& exception) {
            ASSERT_EQ(exception.what(), errorMsg);
        } catch (Exception& e) { FAIL(); }
    }

    static void validateInsertedRels(QueryResult* queryResult, uint64_t numInsertedRels,
        bool containNullValues, bool containLongString) {
        for (auto i = 0u; i < numInsertedRels; i++) {
            auto tuple = queryResult->getNext();
            ASSERT_EQ(tuple->isNull(0), containNullValues && (i % 2 != 0));
            if (!containNullValues || i % 2 == 0) {
                ASSERT_EQ(tuple->getValue(0)->val.int64Val, i);
            }
            ASSERT_EQ(tuple->isNull(1), containNullValues);
            if (!containNullValues) {
                ASSERT_EQ(tuple->getValue(1)->val.strVal.getAsString(),
                    (containLongString ? "long long string prefix " : "") + to_string(i));
            }
            ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0].getAsString(),
                (containLongString ? "long long string prefix " : "") + to_string(i));
        }
    }

    void insertRelsToEmptyListTest(bool isCommit, TransactionTestType transactionTestType,
        uint64_t numRelsToInsert = 100, bool insertNullValues = false,
        bool testLongString = false) {
        insertRelsToKnowsTable(0 /* srcTableID */, 0 /* dstTableID */, numRelsToInsert,
            insertNullValues, testLongString);
        conn->beginWriteTransaction();
        auto result =
            conn->query("match (a:animal)-[e:knows]->(b:animal) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), numRelsToInsert);
        validateInsertedRels(result.get(), numRelsToInsert, insertNullValues, testLongString);
        // Note that the queryResult contains a factorizedTable whose dataBlock is allocated from
        // memoryManager. We must unpin the dataBlock's page before restarting the database, that
        // is to say, we need to manually release the factorizedTable before testing recovery.
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result =
            conn->query("match (a:animal)-[e:knows]->(b:animal) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), isCommit ? numRelsToInsert : 0);
        validateInsertedRels(
            result.get(), isCommit ? numRelsToInsert : 0, insertNullValues, testLongString);
    }

    void validateSmallListAfterInsertion(QueryResult* result, uint64_t numInsertedRels) {
        for (auto i = 0u; i < 51; i++) {
            auto tuple = result->getNext();
            ASSERT_EQ(tuple->getValue(0)->val.int64Val, i);
            auto strVal = getStringValToValidate(1000 - i);
            ASSERT_EQ(tuple->getValue(1)->val.strVal.getAsString(), strVal);
            ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0].getAsString(),
                strVal);
            if (i == 1) {
                validateInsertedRels(result, numInsertedRels, false /* containNullValues */,
                    false /* testLongString */);
            }
        }
    }

    void insertRelsToSmallListTest(
        bool isCommit, TransactionTestType transactionTestType, uint64_t numRelsToInsert = 100) {
        auto numRelsAfterInsertion = 51 + numRelsToInsert;
        insertRelsToKnowsTable(0 /* srcTableID */, 1 /* dstTableID */, numRelsToInsert);
        conn->beginWriteTransaction();
        auto result =
            conn->query("match (:animal)-[e:knows]->(b:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), numRelsAfterInsertion);
        validateSmallListAfterInsertion(result.get(), numRelsToInsert);
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result =
            conn->query("match (:animal)-[e:knows]->(b:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), isCommit ? numRelsAfterInsertion : 51);
        validateSmallListAfterInsertion(result.get(), isCommit ? numRelsToInsert : 0);
    }

    void validateLargeListAfterInsertion(
        QueryResult* result, uint64_t numInsertedRels, bool containNullValues) {
        for (auto i = 0u; i < 2500; i++) {
            auto tuple = result->getNext();
            ASSERT_EQ(tuple->getValue(0)->val.int64Val, 3 * i);
            auto strVal = getStringValToValidate(3000 - i);
            ASSERT_EQ(tuple->getValue(1)->val.strVal.getAsString(), strVal);
            ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0].getAsString(),
                strVal);
        }
        validateInsertedRels(
            result, numInsertedRels, containNullValues, false /* testLongString */);
    }

    void insertRelsToLargeListTest(bool isCommit, TransactionTestType transactionTestType,
        uint64_t numRelsToInsert = 100, bool insertNullValues = false) {
        auto numRelsAfterInsertion = 2500 + numRelsToInsert;
        insertRelsToKnowsTable(
            1 /* srcTableID */, 1 /* dstTableID */, numRelsToInsert, insertNullValues);
        conn->beginWriteTransaction();
        auto result =
            conn->query("match (:person)-[e:knows]->(:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), numRelsAfterInsertion);
        validateLargeListAfterInsertion(result.get(), numRelsToInsert, insertNullValues);
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result =
            conn->query("match (:person)-[e:knows]->(:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), isCommit ? numRelsAfterInsertion : 2500);
        validateLargeListAfterInsertion(
            result.get(), isCommit ? numRelsToInsert : 0, insertNullValues);
    }

    void validatePlanScanBWDList(string query) {
        // This check is to validate whether the query used in "insertRelsAndQueryBWDListTest"
        // scans the backward list of the knows table. If this test fails (eg. we have updated the
        // planner), we should consider changing to a new query which scans the backward list with
        // the new planner.
        auto catalog = database->getCatalog();
        auto statement = Parser::parseQuery(query);
        auto parsedQuery = (RegularQuery*)statement.get();
        auto boundQuery = Binder(*catalog).bind(*parsedQuery);
        auto plan = Planner::getBestPlan(*catalog,
            database->getStorageManager()->getNodesStore().getNodesStatisticsAndDeletedIDs(),
            database->getStorageManager()->getRelsStore().getRelsStatistics(), *boundQuery);
        ASSERT_STREQ(LogicalPlanUtil::encodeJoin(*plan).c_str(), "HJ(a){E(a)S(b)}{S(a)}");
    }

    void insertRelsAndQueryBWDListTest(bool isCommit, TransactionTestType transactionTestType) {
        auto query = "match (a:person)-[e:knows]->(b:animal) return e.length, e.place, e.tag";
        validatePlanScanBWDList(query);
        auto numRelsToInsert = 100;
        insertRelsToKnowsTable(1 /* srcTableID */, 0 /* dstTableID */, numRelsToInsert);
        conn->beginWriteTransaction();
        auto result = conn->query(query);
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), numRelsToInsert);
        validateInsertedRels(result.get(), numRelsToInsert, false /* containNullValues */,
            false /* testLongString */);
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result = conn->query(query);
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), isCommit ? numRelsToInsert : 0);
        if (isCommit) {
            validateInsertedRels(result.get(), numRelsToInsert, false /* containNullValues */,
                false /* testLongString */);
        }
    }

    void insertRelsToNode(node_offset_t srcNodeOffset) {
        auto placeStr = gf_string_t();
        auto tagList = gf_list_t();
        tagList.overflowPtr =
            reinterpret_cast<uint64_t>(tagPropertyVector->getOverflowBuffer().allocateSpace(100));
        tagList.size = 1;
        ((nodeID_t*)srcNodeVector->values)[0] = nodeID_t(srcNodeOffset, 0);
        placeValues[0] = placeStr;
        tagValues[0] = tagList;
        // If the srcNodeOffset is an odd number, we insert 100 rels to it (person 700-799
        // (inclusive)). Otherwise, we insert 1000 rels to it (person 500-1499(inclusive)).
        // Note: inserting 1000 rels to a node will convert the original small list to large list.
        if (srcNodeOffset % 2) {
            for (auto i = 0u; i < 100; i++) {
                auto dstNodeOffset = 700 + srcNodeOffset + i;
                ((nodeID_t*)dstNodeVector->values)[0] = nodeID_t(dstNodeOffset, 1);
                lengthValues[0] = dstNodeOffset * 3;
                placeStr.set(to_string(dstNodeOffset - 5));
                tagList.set((uint8_t*)&placeStr, DataType(LIST, make_unique<DataType>(STRING)));
                insertOneKnowsRel();
            }
        } else {
            for (auto i = 0u; i < 1000; i++) {
                auto dstNodeOffset = 500 + srcNodeOffset + i;
                ((nodeID_t*)dstNodeVector->values)[0] = nodeID_t(dstNodeOffset, 1);
                lengthValues[0] = dstNodeOffset * 2;
                placeStr.set(to_string(dstNodeOffset - 25));
                tagList.set((uint8_t*)&placeStr, DataType(LIST, make_unique<DataType>(STRING)));
                insertOneKnowsRel();
            }
        }
    }

    void validateRelsForNodes(QueryResult* result, bool isCommit) {
        for (auto i = 0u; i < 50; i++) {
            auto tuple = result->getNext();
            // Check rels stored in the persistent store.
            ASSERT_EQ(tuple->getValue(0)->val.int64Val, i);
            auto strVal = getStringValToValidate(1000 - i);
            ASSERT_EQ(tuple->getValue(1)->val.strVal.getAsString(), strVal);
            ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0].getAsString(),
                strVal);
            if (!isCommit) {
                continue;
            }
            if (i % 10 == 0 || i >= 30) {
                // For nodes with nodeOffset 0,10,20 or nodeOffset >= 30, we don't insert new rels
                // for them. So we only need to check the rels in the persistent store.
                return;
            }
            // For other nodes, we need to check the newly inserted edges.
            if (i % 2) {
                for (auto j = 0u; j < 100; j++) {
                    tuple = result->getNext();
                    auto dstNodeOffset = 700 + i + j;
                    ASSERT_EQ(tuple->getValue(0)->val.int64Val, dstNodeOffset * 3);
                    ASSERT_EQ(
                        tuple->getValue(1)->val.strVal.getAsString(), to_string(dstNodeOffset - 5));
                    ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0]
                                  .getAsString(),
                        to_string(dstNodeOffset - 5));
                }
            } else {
                for (auto j = 0u; j < 1000; j++) {
                    tuple = result->getNext();
                    auto dstNodeOffset = 500 + i + j;
                    ASSERT_EQ(tuple->getValue(0)->val.int64Val, dstNodeOffset * 2);
                    ASSERT_EQ(tuple->getValue(1)->val.strVal.getAsString(),
                        to_string(dstNodeOffset - 25));
                    ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0]
                                  .getAsString(),
                        to_string(dstNodeOffset - 25));
                }
            }
        }
    }

    void insertDifferentRelsToNodesTest(bool isCommit, TransactionTestType transactionTestType) {
        // We call insertRelsToNode for 30 node offsets, skipping nodes 0, 10, 20 (the if (i %10)
        // does this). InsertRelsToNode inserts 100 dstNodes for nodes with odd nodeOffset value and
        // insert 1000 dstNodes for nodes with even nodeOffset value. In total, the below code
        // inserts: 15 * 100 + 12 * 1000 = 13500 number of nodes.
        auto numNodesToInsert = 13500;
        auto numNodesInList = 51;
        auto totalNumNodesAfterInsertion = numNodesToInsert + numNodesInList;
        for (auto i = 0u; i < 30; i++) {
            if (i % 10) {
                insertRelsToNode(i);
            }
        }
        conn->beginWriteTransaction();
        auto result =
            conn->query("match (:animal)-[e:knows]->(b:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), totalNumNodesAfterInsertion);
        validateRelsForNodes(result.get(), true /* isCommit */);
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result =
            conn->query("match (:animal)-[e:knows]->(b:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), isCommit ? totalNumNodesAfterInsertion : numNodesInList);
        validateRelsForNodes(result.get(), isCommit);
    }

    void validateSmallListAfterBecomingLarge(QueryResult* queryResult, uint64_t numValuesToInsert) {
        for (auto i = 0u; i < 10; i++) {
            auto tuple = queryResult->getNext();
            ASSERT_EQ(
                tuple->getValue(0)->val.strVal.getAsString(), to_string(i + 1) + to_string(i + 1));
        }
        for (auto i = 0u; i < numValuesToInsert; i++) {
            auto tuple = queryResult->getNext();
            ASSERT_EQ(tuple->getValue(0)->val.strVal.getAsString(), to_string(i));
        }
    }

    // This is to test the case that the size of the stringPropertyList is large than
    // DEFAULT_PAGE_SIZE, and the size of adjList is smaller than DEFAULT_PAGE_SIZE. Note that: our
    // rule is to let the adjList determine whether a list is small or large. As a result, the list
    // should still be small after insertion.
    void smallListBecomesLargeTest(bool isCommit, TransactionTestType transactionTestType) {
        auto numValuesToInsert = 260;
        auto numValuesInList = 10;
        auto totalNumValuesAfterInsertion = numValuesToInsert + numValuesInList;
        auto placeStr = gf_string_t();
        auto relDirections = vector<RelDirection>{RelDirection::FWD, RelDirection::BWD};
        for (auto i = 0u; i < numValuesToInsert; i++) {
            relIDValues[0] = 10 + i;
            placeStr.set(to_string(i));
            placeValues[0] = placeStr;
            ((nodeID_t*)srcNodeVector->values)[0] = nodeID_t(1, 1);
            ((nodeID_t*)dstNodeVector->values)[0] = nodeID_t(i + 1, 1);
            insertOnePlaysRel();
        }
        conn->beginWriteTransaction();
        auto result = conn->query("match (:person)-[p:plays]->(:person) return p.place");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), totalNumValuesAfterInsertion);
        validateSmallListAfterBecomingLarge(result.get(), numValuesToInsert);
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result = conn->query("match (:person)-[p:plays]->(:person) return p.place");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), isCommit ? totalNumValuesAfterInsertion : 10);
        validateSmallListAfterBecomingLarge(result.get(), isCommit ? numValuesToInsert : 0);
    }

    void validateOneToOneRelTableAfterInsertion(
        bool afterRollback, QueryResult* queryResult, bool testLongString, bool testNull) {
        ASSERT_TRUE(queryResult->isSuccess());
        // The hasOwner relTable has 10 rels in persistent store, and 2 rels in updateStore. If we
        // don't rollback the transaction, the teaches relTable should have 12 rels.
        auto numRelsAfterInsertion = afterRollback ? 10 : 12;
        ASSERT_EQ(queryResult->getNumTuples(), numRelsAfterInsertion);
        for (auto i = 0u; i < numRelsAfterInsertion; i++) {
            // If we have rollback the transaction, we shouldn't see the inserted rels, which are:
            // Animal0->person50, Animal10->person60.
            if (afterRollback && i % 10 == 0) {
                continue;
            }
            auto tuple = queryResult->getNext();
            // If we are testing nullValues, the length and place property will be null if this edge
            // is an inserted edge(checked by i % 10) and the srcNodeOffset is not a multiple of
            // 20(checked by i % 20).
            auto isNull = i % 10 == 0 && (testNull && i % 20);
            ASSERT_EQ(tuple->isNull(0), isNull);
            if (!isNull) {
                // If this is an inserted edge(checked by i % 10), the length property value
                // is i / 10. Otherwise, the length property value is i.
                ASSERT_EQ(tuple->getValue(0)->val.int64Val, i % 10 ? i : i / 10);
            }
            ASSERT_EQ(tuple->isNull(1), isNull);
            if (!isNull) {
                // If this is an inserted edge(checked by i % 10), the place property value
                // is prefix(if we are testing long strings) + i / 10. Otherwise,
                // the place property value is "2000-i"(srcNodeOffset is odd) or 3 *
                // "2000-i"(srcNodeOffset is even).
                auto strVal =
                    i % 10 ?
                        (i % 2 ? to_string(2000 - i) :
                                 to_string(2000 - i) + to_string(2000 - i) + to_string(2000 - i)) :
                        ((testLongString && i % 20 ? "long string prefix " : "") +
                            to_string(i / 10));
                ASSERT_EQ(tuple->getValue(1)->val.strVal.getAsString(), strVal);
            }
        }
    }

    void insertRelsToOneToOneRelTable(bool isCommit, TransactionTestType transactionTestType,
        bool testLongString = false, bool testNull = false) {
        auto query = "match (a:animal)-[h:hasOwner]->(:person) return h.length, h.place";
        auto placeStr = gf_string_t();
        placeStr.overflowPtr =
            reinterpret_cast<uint64_t>(tagPropertyVector->getOverflowBuffer().allocateSpace(100));
        // We insert 2 rels to eHasOwner:
        // 1. Animal0->Person50, length: 0, place: "2000".
        // 2. Animal10->Person60, length: 10, place: "1990".
        // If we are testing long string, we also add the prefix "long string prefix" to place str.
        for (auto i = 0u; i < 2; i++) {
            relIDValues[0] = 10 + i;
            lengthValues[0] = i;
            placeStr.set((testLongString && i % 2 ? "long string prefix " : "") + to_string(i));
            placeValues[0] = placeStr;
            lengthPropertyVector->setNull(0, testNull && i % 2);
            placePropertyVector->setNull(0, testNull && i % 2);
            ((nodeID_t*)srcNodeVector->values)[0] = nodeID_t(i * 10, 0);
            ((nodeID_t*)dstNodeVector->values)[0] = nodeID_t(i * 10 + 50, 1);
            insertOneHasOwnerRel();
        }
        conn->beginWriteTransaction();
        auto result = conn->query(query);
        validateOneToOneRelTableAfterInsertion(
            false /* afterRollback */, result.get(), testLongString, testNull);
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result = conn->query(query);
        validateOneToOneRelTableAfterInsertion(!isCommit, result.get(), testLongString, testNull);
    }

    void validateManyToOneRelTableAfterInsertion(bool afterRollback, QueryResult* queryResult) {
        ASSERT_TRUE(queryResult->isSuccess());
        // The teaches relTable has 6 rels in persistent store, and 3 rels in updateStore. If we
        // don't rollback the transaction, the teaches relTable should have 9 rels.
        auto numRelsAfterInsertion = afterRollback ? 6 : 9;
        ASSERT_EQ(queryResult->getNumTuples(), numRelsAfterInsertion);
        shared_ptr<FlatTuple> tuple;
        for (auto i = 1u; i <= 3; i++) {
            for (auto j = 0; j <= i; j++) {
                // If we have rollbacked the transaction, there will be no edges for
                // person10->person1,person20->person2,person30->person3.
                if (afterRollback && j == 0) {
                    continue;
                } else {
                    tuple = queryResult->getNext();
                }
                // The length property's value is equal to the srcPerson's ID.
                ASSERT_EQ(tuple->getValue(0)->val.int64Val, i * 10 + j);
            }
        }
    }

    // For many-to-one rel tables, the FWD is a list and the bwd is a column. For the FWD, we need
    // to write the inserted rels to the WAL version of the pages of the column. For the BWD, we
    // need to write the inserted rels to adjAndPropertyListsUpdateStore. This test is designed
    // to test updates to a relTable which has a mixed of columns and lists.
    void insertRelsToManyToOneRelTable(bool isCommit, TransactionTestType transactionTestType) {
        auto query = "match (:person)-[t:teaches]->(:person) return t.length";
        // We insert 3 edges to teaches relTable:
        // 1. person10->person1, with length property 10.
        // 2. person20->person2, with length property 20.
        // 3. person30->person3, with lenght property 30.
        for (auto i = 0u; i < 3; i++) {
            relIDValues[0] = 10 + i;
            lengthValues[0] = (i + 1) * 10;
            ((nodeID_t*)srcNodeVector->values)[0] = nodeID_t(10 * (i + 1), 1);
            ((nodeID_t*)dstNodeVector->values)[0] = nodeID_t(i + 1, 1);
            insertOneTeachesRel();
        }
        conn->beginWriteTransaction();
        auto result = conn->query(query);
        validateManyToOneRelTableAfterInsertion(false /* afterRollback */, result.get());
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result = conn->query(query);
        validateManyToOneRelTableAfterInsertion(!isCommit, result.get());
    }

public:
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    shared_ptr<ValueVector> relIDPropertyVector;
    int64_t* relIDValues;
    shared_ptr<ValueVector> lengthPropertyVector;
    int64_t* lengthValues;
    shared_ptr<ValueVector> placePropertyVector;
    gf_string_t* placeValues;
    shared_ptr<ValueVector> tagPropertyVector;
    gf_list_t* tagValues;
    shared_ptr<ValueVector> srcNodeVector;
    shared_ptr<ValueVector> dstNodeVector;
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(6 /* numValueVectors */);
    vector<shared_ptr<ValueVector>> vectorsToInsertToKnows;
    vector<shared_ptr<ValueVector>> vectorsToInsertToPlays;
    vector<shared_ptr<ValueVector>> vectorsToInsertToHasOwner;
    vector<shared_ptr<ValueVector>> vectorsToInsertToTeaches;
};

TEST_F(RelInsertionTest, InsertRelsToEmptyListCommitNormalExecution) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToEmptyListCommitRecovery) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToEmptyListRollbackNormalExecution) {
    insertRelsToEmptyListTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToEmptyListRollbackRecovery) {
    insertRelsToEmptyListTest(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToEmptyListWithNullCommitNormalExecution) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION,
        100 /* numRelsToInsert */, true /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertRelsToEmptyListWithNullCommitRecovery) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::RECOVERY,
        100 /* numRelsToInsert */, true /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListCommitNormalExecution) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION,
        510 /* numRelsToInsert */, false /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListCommitRecovery) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::RECOVERY,
        510 /* numRelsToInsert */, false /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListRollbackNormalExecution) {
    insertRelsToEmptyListTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION,
        510 /* numRelsToInsert */, false /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListRollbackRecovery) {
    insertRelsToEmptyListTest(false /* isCommit */, TransactionTestType::RECOVERY,
        510 /* numRelsToInsert */, false /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListWithNullCommitNormalExecution) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION,
        510 /* numRelsToInsert */, true /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListWithNullCommitRecovery) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::RECOVERY,
        510 /* numRelsToInsert */, true /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListLongStringCommitNormalExecution) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION,
        510 /* numRelsToInsert */, false /* insertNullValues */, true /* testLongString */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListLongStringCommitRecovery) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::RECOVERY,
        510 /* numRelsToInsert */, false /* insertNullValues */, true /* testLongString */);
}

TEST_F(RelInsertionTest, InsertRelsToSmallListCommitNormalExecution) {
    insertRelsToSmallListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToSmallListCommitRecovery) {
    insertRelsToSmallListTest(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToSmallListRollbackNormalExecution) {
    insertRelsToSmallListTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToSmallListRollbackRecovery) {
    insertRelsToSmallListTest(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToSmallListCommitNormalExecution) {
    insertRelsToSmallListTest(
        true /* isCommit */, TransactionTestType::NORMAL_EXECUTION, 1000 /* numRelsToInsert */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToSmallListCommitRecovery) {
    insertRelsToSmallListTest(
        true /* isCommit */, TransactionTestType::RECOVERY, 1000 /* numRelsToInsert */);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListRollbackNormalExecution) {
    insertRelsToLargeListTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListRollbackRecovery) {
    insertRelsToLargeListTest(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListCommitNormalExecution) {
    insertRelsToLargeListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListCommitRecovery) {
    insertRelsToLargeListTest(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListWithNullCommitNormalExecution) {
    insertRelsToLargeListTest(
        true /* isCommit */, TransactionTestType::NORMAL_EXECUTION, true /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListWithNullCommitRecovery) {
    insertRelsToLargeListTest(
        true /* isCommit */, TransactionTestType::RECOVERY, true /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertRelsAndQueryBWDListCommitNormalExecution) {
    insertRelsAndQueryBWDListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsAndQueryBWDListRollbackNormalExecution) {
    insertRelsAndQueryBWDListTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsAndQueryBWDListCommitRecovery) {
    insertRelsAndQueryBWDListTest(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsAndQueryBWDListRollbackRecovery) {
    insertRelsAndQueryBWDListTest(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToDifferentNodesCommitNormalExecution) {
    insertDifferentRelsToNodesTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToDifferentNodesRollbackNormalExecution) {
    insertDifferentRelsToNodesTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToDifferentNodesCommitRecovery) {
    insertDifferentRelsToNodesTest(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToDifferentNodesRollbackRecovery) {
    insertDifferentRelsToNodesTest(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, smallListBecomesLargeCommitNormalExecution) {
    smallListBecomesLargeTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, SmallListBecomesLargeRollbackNormalExecution) {
    smallListBecomesLargeTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, SmallListBecomesLargeCommitRecovery) {
    smallListBecomesLargeTest(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, SmallListBecomesLargeRollbackRecovery) {
    smallListBecomesLargeTest(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InCorrectVectorErrorTest) {
    incorrectVectorErrorTest(vector<shared_ptr<ValueVector>>{lengthPropertyVector,
                                 placePropertyVector, relIDPropertyVector, srcNodeVector,
                                 relIDPropertyVector, tagPropertyVector, dstNodeVector},
        "Expected number of valueVectors: 6. Given: 7.");
    incorrectVectorErrorTest(
        vector<shared_ptr<ValueVector>>{srcNodeVector, dstNodeVector, placePropertyVector,
            lengthPropertyVector, tagPropertyVector, relIDPropertyVector},
        "Expected vector with type INT64, Given: STRING.");
    incorrectVectorErrorTest(
        vector<shared_ptr<ValueVector>>{srcNodeVector, relIDPropertyVector, lengthPropertyVector,
            placePropertyVector, tagPropertyVector, relIDPropertyVector},
        "The first two vectors of srcDstNodeIDAndRelProperties should be src/dstNodeVector.");
    ((nodeID_t*)srcNodeVector->values)[0].tableID = 5;
    incorrectVectorErrorTest(
        vector<shared_ptr<ValueVector>>{srcNodeVector, dstNodeVector, lengthPropertyVector,
            placePropertyVector, tagPropertyVector, relIDPropertyVector},
        "TableID: 5 is not a valid src tableID in rel knows.");
}

TEST_F(RelInsertionTest, InsertRelsToOneToOneRelTableCommitNormalExecution) {
    insertRelsToOneToOneRelTable(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToOneToOneRelTableRollbackNormalExecution) {
    insertRelsToOneToOneRelTable(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToOneToOneRelTableCommitRecovery) {
    insertRelsToOneToOneRelTable(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToOneToOneRelTableRollbackRecovery) {
    insertRelsToOneToOneRelTable(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertLongStringsToOneToOneRelTableCommitNormalExecution) {
    insertRelsToOneToOneRelTable(
        true /* isCommit */, TransactionTestType::NORMAL_EXECUTION, true /* testLongStrings */);
}

TEST_F(RelInsertionTest, InsertLongStringsToOneToOneRelTableRollbackNormalExecution) {
    insertRelsToOneToOneRelTable(
        false /* isCommit */, TransactionTestType::NORMAL_EXECUTION, true /* testLongStrings */);
}

TEST_F(RelInsertionTest, InsertLongStringsToOneToOneRelTableCommitRecovery) {
    insertRelsToOneToOneRelTable(
        true /* isCommit */, TransactionTestType::RECOVERY, true /* testLongStrings */);
}

TEST_F(RelInsertionTest, InsertLongStringsToOneToOneRelTableRollbackRecovery) {
    insertRelsToOneToOneRelTable(
        false /* isCommit */, TransactionTestType::RECOVERY, true /* testLongStrings */);
}

TEST_F(RelInsertionTest, InsertNullValuesToOneToOneRelTableCommitNormalExecution) {
    insertRelsToOneToOneRelTable(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION,
        false /* testLongStrings */, true /* testNulls */);
}

TEST_F(RelInsertionTest, InsertNullValuesToOneToOneRelTableRollbackNormalExecution) {
    insertRelsToOneToOneRelTable(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION,
        false /* testLongStrings */, true /* testNulls */);
}

TEST_F(RelInsertionTest, InsertNullValuesToOneToOneRelTableCommitRecovery) {
    insertRelsToOneToOneRelTable(true /* isCommit */, TransactionTestType::RECOVERY,
        false /* testLongStrings */, true /* testNulls */);
}

TEST_F(RelInsertionTest, InsertNullValuesToOneToOneRelTableRollbackRecovery) {
    insertRelsToOneToOneRelTable(false /* isCommit */, TransactionTestType::RECOVERY,
        false /* testLongStrings */, true /* testNulls */);
}

TEST_F(RelInsertionTest, InsertRelsToManyToOneRelTableCommitNormalExecution) {
    insertRelsToManyToOneRelTable(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToManyToOneRelTableRollbackNormalExecution) {
    insertRelsToManyToOneRelTable(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToManyToOneRelTableCommitRecovery) {
    insertRelsToManyToOneRelTable(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToManyToOneRelTableRollbackRecovery) {
    insertRelsToManyToOneRelTable(false /* isCommit */, TransactionTestType::RECOVERY);
}
