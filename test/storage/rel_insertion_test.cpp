#include "parser/parser.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/planner.h"
#include "test_helper/test_helper.h"

using namespace kuzu::testing;

class RelInsertionTest : public DBTest {

public:
    void SetUp() override {
        bufferManager =
            make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
        DBTest::SetUp();
        // Set tableIDs
        auto catalogContents = getCatalog(*database)->getReadOnlyVersion();
        ANIMAL_TABLE_ID = catalogContents->getNodeTableIDFromName("animal");
        PERSON_TABLE_ID = catalogContents->getNodeTableIDFromName("person");
        KNOWS_TABLE_ID = catalogContents->getRelTableIDFromName("knows");
        PLAYS_TABLE_ID = catalogContents->getRelTableIDFromName("plays");
        HAS_OWNER_TABLE_ID = catalogContents->getRelTableIDFromName("hasOwner");
        TEACHES_TABLE_ID = catalogContents->getRelTableIDFromName("teaches");

        // Set vectors
        relIDPropertyVector = make_shared<ValueVector>(INT64, memoryManager.get());
        relIDValues = (int64_t*)relIDPropertyVector->getData();
        lengthPropertyVector = make_shared<ValueVector>(INT64, memoryManager.get());
        lengthValues = (int64_t*)lengthPropertyVector->getData();
        placePropertyVector = make_shared<ValueVector>(STRING, memoryManager.get());
        placeValues = (ku_string_t*)placePropertyVector->getData();
        srcNodeVector = make_shared<ValueVector>(NODE_ID, memoryManager.get());
        dstNodeVector = make_shared<ValueVector>(NODE_ID, memoryManager.get());
        tagPropertyVector = make_shared<ValueVector>(
            DataType{LIST, make_unique<DataType>(STRING)}, memoryManager.get());
        tagValues = (ku_list_t*)tagPropertyVector->getData();
        dataChunk->insert(0, relIDPropertyVector);
        dataChunk->insert(1, lengthPropertyVector);
        dataChunk->insert(2, placePropertyVector);
        dataChunk->insert(3, tagPropertyVector);
        dataChunk->insert(4, srcNodeVector);
        dataChunk->insert(5, dstNodeVector);
        dataChunk->state->currIdx = 0;
        vectorsToInsertToKnows = vector<shared_ptr<ValueVector>>{
            lengthPropertyVector, placePropertyVector, tagPropertyVector, relIDPropertyVector};
        vectorsToInsertToPlays =
            vector<shared_ptr<ValueVector>>{placePropertyVector, relIDPropertyVector};
        vectorsToInsertToHasOwner = vector<shared_ptr<ValueVector>>{
            lengthPropertyVector, placePropertyVector, relIDPropertyVector};
        vectorsToInsertToTeaches =
            vector<shared_ptr<ValueVector>>{lengthPropertyVector, relIDPropertyVector};
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

    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/rel-insertion-tests/");
    }

    void validateQueryBestPlanJoinOrder(string query, string expectedJoinOrder) {
        auto catalog = getCatalog(*database);
        auto statement = Parser::parseQuery(query);
        auto parsedQuery = (RegularQuery*)statement.get();
        auto boundQuery = Binder(*catalog).bind(*parsedQuery);
        auto plan = Planner::getBestPlan(*catalog,
            getStorageManager(*database)->getNodesStore().getNodesStatisticsAndDeletedIDs(),
            getStorageManager(*database)->getRelsStore().getRelsStatistics(), *boundQuery);
        ASSERT_STREQ(LogicalPlanUtil::encodeJoin(*plan).c_str(), expectedJoinOrder.c_str());
    }

    inline void insertOneRelToRelTable(table_id_t relTableID,
        shared_ptr<ValueVector>& srcNodeVector_, shared_ptr<ValueVector>& dstNodeVector_,
        vector<shared_ptr<ValueVector>>& propertyVectors) {
        getStorageManager(*database)
            ->getRelsStore()
            .getRelTable(relTableID)
            ->insertRels(srcNodeVector_, dstNodeVector_, propertyVectors);
    }

    inline void insertOneKnowsRel() {
        insertOneRelToRelTable(
            KNOWS_TABLE_ID, srcNodeVector, dstNodeVector, vectorsToInsertToKnows);
    }

    inline void insertOnePlaysRel() {
        insertOneRelToRelTable(
            PLAYS_TABLE_ID, srcNodeVector, dstNodeVector, vectorsToInsertToPlays);
    }

    inline void insertOneHasOwnerRel() {
        insertOneRelToRelTable(
            HAS_OWNER_TABLE_ID, srcNodeVector, dstNodeVector, vectorsToInsertToHasOwner);
    }

    inline void insertOneTeachesRel() {
        insertOneRelToRelTable(
            TEACHES_TABLE_ID, srcNodeVector, dstNodeVector, vectorsToInsertToTeaches);
    }

    void insertRelsToKnowsTable(table_id_t srcTableID, table_id_t dstTableID,
        uint64_t numValuesToInsert = 100, bool insertNullValues = false,
        bool testLongString = false) {
        auto placeStr = ku_string_t();
        if (testLongString) {
            placeStr.overflowPtr = reinterpret_cast<uint64_t>(
                placePropertyVector->getOverflowBuffer().allocateSpace(100));
        }
        auto tagList = ku_list_t();
        tagList.overflowPtr =
            reinterpret_cast<uint64_t>(tagPropertyVector->getOverflowBuffer().allocateSpace(100));
        tagList.size = 1;
        for (auto i = 0u; i < numValuesToInsert; i++) {
            relIDValues[0] = 1051 + i;
            lengthValues[0] = i;
            placeStr.set((testLongString ? "" : "") + to_string(i));
            placeValues[0] = placeStr;
            tagList.set((uint8_t*)&placeStr, DataType(LIST, make_unique<DataType>(STRING)));
            tagValues[0] = tagList;
            if (insertNullValues) {
                lengthPropertyVector->setNull(0, i % 2);
                placePropertyVector->setNull(0, true /* isNull */);
            }
            nodeID_t srcNodeID(1 /* offset */, srcTableID), dstNodeID(i + 1, dstTableID);
            srcNodeVector->setValue(0, srcNodeID);
            dstNodeVector->setValue(0, dstNodeID);
            insertOneKnowsRel();
        }
    }

    static void validateInsertedRels(QueryResult* queryResult, uint64_t numInsertedRels,
        bool containNullValues, bool containLongString) {
        for (auto i = 0u; i < numInsertedRels; i++) {
            auto tuple = queryResult->getNext();
            ASSERT_EQ(tuple->getResultValue(0)->isNullVal(), containNullValues && (i % 2 != 0));
            if (!containNullValues || i % 2 == 0) {
                ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), i);
            }
            ASSERT_EQ(tuple->getResultValue(1)->isNullVal(), containNullValues);
            if (!containNullValues) {
                ASSERT_EQ(tuple->getResultValue(1)->getStringVal(),
                    (containLongString ? "long long string prefix " : "") + to_string(i));
            }
            ASSERT_EQ((tuple->getResultValue(2)->getListVal())[0].getStringVal(),
                (containLongString ? "long long string prefix " : "") + to_string(i));
        }
    }

    void insertRelsAndQueryBWDListTest(bool isCommit, TransactionTestType transactionTestType) {
        auto query = "match (a:person)-[e:knows]->(b:animal) return e.length, e.place, e.tag";
        // This check is to validate whether the query used in "insertRelsAndQueryBWDListTest"
        // scans the backward list of the knows table. If this test fails (eg. we have updated the
        // planner), we should consider changing to a new query which scans the BWD list with
        // the new planner.
        validateQueryBestPlanJoinOrder(query, "HJ(a){E(a)S(b)}{S(a)}");
        auto numRelsToInsert = 100;
        insertRelsToKnowsTable(PERSON_TABLE_ID, ANIMAL_TABLE_ID, numRelsToInsert);
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
        auto placeStr = ku_string_t();
        auto tagList = ku_list_t();
        tagList.overflowPtr =
            reinterpret_cast<uint64_t>(tagPropertyVector->getOverflowBuffer().allocateSpace(100));
        tagList.size = 1;
        nodeID_t srcNodeID(srcNodeOffset, 0);
        srcNodeVector->setValue(0, srcNodeID);
        placeValues[0] = placeStr;
        tagValues[0] = tagList;
        // If the srcNodeOffset is an odd number, we insert 100 rels to it (person 700-799
        // (inclusive)). Otherwise, we insert 1000 rels to it (person 500-1499(inclusive)).
        // Note: inserting 1000 rels to a node will convert the original small list to large list.
        if (srcNodeOffset % 2) {
            for (auto i = 0u; i < 100; i++) {
                auto dstNodeOffset = 700 + srcNodeOffset + i;
                nodeID_t dstNodeID(dstNodeOffset, 1);
                dstNodeVector->setValue(0, dstNodeID);
                lengthValues[0] = dstNodeOffset * 3;
                placeStr.set(to_string(dstNodeOffset - 5));
                tagList.set((uint8_t*)&placeStr, DataType(LIST, make_unique<DataType>(STRING)));
                insertOneKnowsRel();
            }
        } else {
            for (auto i = 0u; i < 1000; i++) {
                auto dstNodeOffset = 500 + srcNodeOffset + i;
                nodeID_t dstNodeID(dstNodeOffset, 1);
                dstNodeVector->setValue(0, dstNodeID);
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
            ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), i);
            auto strVal = getStringValToValidate(1000 - i);
            ASSERT_EQ(tuple->getResultValue(1)->getStringVal(), strVal);
            ASSERT_EQ((tuple->getResultValue(2)->getListVal())[0].getStringVal(), strVal);
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
                    ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), dstNodeOffset * 3);
                    ASSERT_EQ(
                        tuple->getResultValue(1)->getStringVal(), to_string(dstNodeOffset - 5));
                    ASSERT_EQ((tuple->getResultValue(2)->getListVal())[0].getStringVal(),
                        to_string(dstNodeOffset - 5));
                }
            } else {
                for (auto j = 0u; j < 1000; j++) {
                    tuple = result->getNext();
                    auto dstNodeOffset = 500 + i + j;
                    ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), dstNodeOffset * 2);
                    ASSERT_EQ(
                        tuple->getResultValue(1)->getStringVal(), to_string(dstNodeOffset - 25));
                    ASSERT_EQ((tuple->getResultValue(2)->getListVal())[0].getStringVal(),
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
                tuple->getResultValue(0)->getStringVal(), to_string(i + 1) + to_string(i + 1));
        }
        for (auto i = 0u; i < numValuesToInsert; i++) {
            auto tuple = queryResult->getNext();
            ASSERT_EQ(tuple->getResultValue(0)->getStringVal(), to_string(i));
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
        auto placeStr = ku_string_t();
        auto relDirections = vector<RelDirection>{RelDirection::FWD, RelDirection::BWD};
        for (auto i = 0u; i < numValuesToInsert; i++) {
            relIDValues[0] = 10 + i;
            placeStr.set(to_string(i));
            placeValues[0] = placeStr;
            nodeID_t srcNodeID(1, 1), dstNodeID(i + 1, 1);
            srcNodeVector->setValue(0, srcNodeID);
            dstNodeVector->setValue(0, dstNodeID);
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
            ASSERT_EQ(tuple->getResultValue(0)->isNullVal(), isNull);
            if (!isNull) {
                // If this is an inserted edge(checked by i % 10), the length property value
                // is i / 10. Otherwise, the length property value is i.
                ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), i % 10 ? i : i / 10);
            }
            ASSERT_EQ(tuple->getResultValue(1)->isNullVal(), isNull);
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
                ASSERT_EQ(tuple->getResultValue(1)->getStringVal(), strVal);
            }
        }
    }

    void insertRelsToOneToOneRelTable(bool isCommit, TransactionTestType transactionTestType,
        bool testLongString = false, bool testNull = false) {
        auto query = "match (a:animal)-[h:hasOwner]->(:person) return h.length, h.place";
        auto placeStr = ku_string_t();
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
            nodeID_t srcNodeID(i * 10, 0), dstNodeID(i * 10 + 50, i);
            srcNodeVector->setValue(0, srcNodeID);
            dstNodeVector->setValue(0, dstNodeID);
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
                ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), i * 10 + j);
            }
        }
    }

    // For many-to-one rel tables, the FWD is a list and the bwd is a column. For the FWD, we need
    // to write the inserted rels to the WAL version of the pages of the column. For the BWD, we
    // need to write the inserted rels to listsUpdateStore. This test is designed
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
            ((nodeID_t*)srcNodeVector->getData())[0] = nodeID_t(10 * (i + 1), 1);
            ((nodeID_t*)dstNodeVector->getData())[0] = nodeID_t(i + 1, 1);
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

    void insertNodesToPersonTable() {
        for (auto i = 0u; i < 100; ++i) {
            // We already have 2501 nodes in person table, so the next node ID starts with 2501.
            auto id = 2501 + i;
            auto result = conn->query("CREATE (a:person {ID: " + to_string(id) + "})");
            ASSERT_TRUE(result->isSuccess());
        }
        auto result = conn->query("MATCH (p:person) return p.ID");
        ASSERT_TRUE(result->isSuccess());
        // The initial person table has 2501 nodes, and we insert 100 person nodes. In total, we
        // should have 2601 nodes after insertion.
        ASSERT_EQ(result->getNumTuples(), 2601);
    }

    void queryRelsOfPerson(bool afterRollback) {
        // Test whether we set the emptyList correctly in the MANY-MANY relTable.
        auto result = conn->query("MATCH (p:person)-[e:knows]->(:person) return p.ID");
        auto numInsertedRels = 10;
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(
            result->getNumTuples(), 2500 /* numRels in knows table for person knows person */ +
                                        (afterRollback ? 0 : numInsertedRels));
        // Test whether we set the emptyList correctly in the ONE-ONE relTable.
        result = conn->query("MATCH (:animal)-[:hasOwner]->(p:person) return p.ID");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(
            result->getNumTuples(), 10 /* numRels in hasOwner table for animal hasOwner person */ +
                                        (afterRollback ? 0 : numInsertedRels));
        // Test whether we set the emptyList correctly in the MANY-ONE relTable.
        result = conn->query("MATCH (:person)-[:teaches]->(p:person) return p.ID");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(
            result->getNumTuples(), 6 /* numRels in teaches table for person teaches person */ +
                                        (afterRollback ? 0 : numInsertedRels));
        // We want to make sure that this query scans the BWD list of person. If the planner
        // changes, we should consider making another query that scans the BWD list.
        validateQueryBestPlanJoinOrder(
            "MATCH (p1:person)-[:teaches]->(p2:person) return p1.ID", "HJ(p2){E(p2)S(p1)}{S(p2)}");
    }

    void insertNodesToPersonAndQueryRels(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        insertNodesToPersonTable();
        auto& srcNode = ((nodeID_t*)(srcNodeVector->getData()))[0];
        auto& dstNode = ((nodeID_t*)(dstNodeVector->getData()))[0];
        auto placeStr = ku_string_t();
        auto tagList = ku_list_t();
        tagList.overflowPtr =
            reinterpret_cast<uint64_t>(tagPropertyVector->getOverflowBuffer().allocateSpace(100));
        tagList.size = 1;
        for (auto i = 0u; i < 10; i++) {
            relIDValues[0] = 2000 + i;
            lengthValues[0] = i;
            placeStr.set(to_string(i));
            placeValues[0] = placeStr;
            tagList.set((uint8_t*)&placeStr, DataType(LIST, make_unique<DataType>(STRING)));
            tagValues[0] = tagList;
            // Insert 10 rels from person->person to knows/plays/teaches relTables.
            srcNode.tableID = PERSON_TABLE_ID;
            srcNode.offset = 40 + i;
            dstNode.tableID = PERSON_TABLE_ID;
            dstNode.offset = 40 + i;
            insertOneKnowsRel();
            insertOneTeachesRel();
            // Insert 10 rels from animal->person to hasOwner relTable.
            srcNode.tableID = ANIMAL_TABLE_ID;
            srcNode.offset = 20 + i;
            insertOneHasOwnerRel();
        }
        queryRelsOfPerson(false /* afterRollback */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        queryRelsOfPerson(!isCommit);
    }

public:
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    shared_ptr<ValueVector> relIDPropertyVector;
    int64_t* relIDValues;
    shared_ptr<ValueVector> lengthPropertyVector;
    int64_t* lengthValues;
    shared_ptr<ValueVector> placePropertyVector;
    ku_string_t* placeValues;
    shared_ptr<ValueVector> tagPropertyVector;
    ku_list_t* tagValues;
    shared_ptr<ValueVector> srcNodeVector;
    shared_ptr<ValueVector> dstNodeVector;
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(6 /* numValueVectors */);
    vector<shared_ptr<ValueVector>> vectorsToInsertToKnows;
    vector<shared_ptr<ValueVector>> vectorsToInsertToPlays;
    vector<shared_ptr<ValueVector>> vectorsToInsertToHasOwner;
    vector<shared_ptr<ValueVector>> vectorsToInsertToTeaches;
    table_id_t ANIMAL_TABLE_ID, PERSON_TABLE_ID, KNOWS_TABLE_ID, PLAYS_TABLE_ID, HAS_OWNER_TABLE_ID,
        TEACHES_TABLE_ID;
};

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

TEST_F(RelInsertionTest, NewNodesHaveEmptyRelsCommitNormalExecution) {
    insertNodesToPersonAndQueryRels(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, NewNodesHaveEmptyRelsRollbackNormalExecution) {
    insertNodesToPersonAndQueryRels(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, NewNodesHaveEmptyRelsCommitRecovery) {
    insertNodesToPersonAndQueryRels(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, NewNodesHaveEmptyRelsRollbackRecovery) {
    insertNodesToPersonAndQueryRels(false /* isCommit */, TransactionTestType::RECOVERY);
}
