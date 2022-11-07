#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

#include "src/common/include/csv_reader/csv_reader.h"
#include "src/storage/index/include/hash_index.h"

using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

// TODO(Guodong): Add the support of string keys .

class HashIndexTest : public DBTest {
public:
    static constexpr uint64_t NUM_KEYS_FROM_CSV = 10000;

    HashIndexTest() : IDIndex{nullptr} {}

    void SetUp() override {
        DBTest::SetUp();
        initDBAndConnection();
    }

    string getInputCSVDir() override { return "dataset/node-insertion-deletion-tests/"; }

    void initDBAndConnection() {
        createDBAndConn();
        readConn = make_unique<Connection>(database.get());
        table_id_t personTableID =
            database->getCatalog()->getReadOnlyVersion()->getNodeTableIDFromName("person");
        IDIndex = database->getStorageManager()
                      ->getNodesStore()
                      .getNodeTable(personTableID)
                      ->getIDIndex();
    }

    void commitOrRollbackConnectionAndInitDBIfNecessary(
        bool isCommit, TransactionTestType transactionTestType) {
        commitOrRollbackConnection(isCommit, transactionTestType);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            // This creates a new database/conn/readConn and should run the recovery algorithm
            initDBAndConnection();
            conn->beginWriteTransaction();
        }
    }

    static void verifyAfterInsertions(
        Transaction* trx, HashIndex* IDIndex, uint32_t numKeysToInsert, bool assertTrue) {
        node_offset_t result;
        if (assertTrue) {
            for (auto i = 0u; i < numKeysToInsert; i++) {
                uint64_t key = NUM_KEYS_FROM_CSV + i;
                ASSERT_TRUE(IDIndex->lookup(trx, key, result));
                ASSERT_EQ(result, key << 1);
            }
        } else {
            for (auto i = 0u; i < numKeysToInsert; i++) {
                uint64_t key = NUM_KEYS_FROM_CSV + i;
                ASSERT_FALSE(IDIndex->lookup(trx, key, result));
            }
        }
    }

    void insertTest(bool isCommit, TransactionTestType transactionTestType) {
        auto numKeysToInsert = 1000;
        conn->beginWriteTransaction();
        for (auto i = 0u; i < numKeysToInsert; i++) {
            uint64_t key = NUM_KEYS_FROM_CSV + i;
            ASSERT_TRUE(IDIndex->insert(conn->getActiveTransaction(), key, key << 1));
        }
        if (isCommit) {
            // Lookup with read transaction before committing.
            readConn->beginReadOnlyTransaction();
            verifyAfterInsertions(readConn->getActiveTransaction(), IDIndex, numKeysToInsert,
                false /* assert false*/);
            readConn->commit();
        } else {
            // Lookup with write transaction before rolling back.
            verifyAfterInsertions(
                conn->getActiveTransaction(), IDIndex, numKeysToInsert, true /* assert true */);
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        // Lookup with read transaction.
        readConn->beginReadOnlyTransaction();
        if (isCommit) {
            verifyAfterInsertions(
                readConn->getActiveTransaction(), IDIndex, numKeysToInsert, true /* assert true */);
            verifyAfterInsertions(
                conn->getActiveTransaction(), IDIndex, numKeysToInsert, true /* assert true */);
        } else {
            verifyAfterInsertions(readConn->getActiveTransaction(), IDIndex, numKeysToInsert,
                false /* assert false */);
            verifyAfterInsertions(
                conn->getActiveTransaction(), IDIndex, numKeysToInsert, false /* assert false */);
        }
        readConn->commit();
        conn->commit();
    }

    static void verifyAfterDeletions(
        Transaction* trx, HashIndex* IDIndex, uint32_t numKeys, bool assertTrue) {
        node_offset_t result;
        if (assertTrue) {
            for (auto i = 0u; i < numKeys; i++) {
                ASSERT_TRUE(IDIndex->lookup(trx, i, result));
            }
        } else {
            for (auto i = 0u; i < numKeys; i++) {
                if (i % 2 == 0) {
                    ASSERT_FALSE(IDIndex->lookup(trx, i, result));
                } else {
                    ASSERT_TRUE(IDIndex->lookup(trx, i, result));
                }
            }
        }
    }

    void deleteTest(bool isCommit, TransactionTestType transactionTestType) {
        node_offset_t result;
        conn->beginWriteTransaction();
        for (auto i = 0u; i < NUM_KEYS_FROM_CSV; i++) {
            if (i % 2 == 0) {
                IDIndex->deleteKey(conn->getActiveTransaction(), i);
            }
        }
        if (isCommit) {
            // Lookup with read transaction before committing.
            readConn->beginReadOnlyTransaction();
            verifyAfterDeletions(readConn->getActiveTransaction(), IDIndex, NUM_KEYS_FROM_CSV,
                true /* assert true */);
            readConn->commit();
        } else {
            // Lookup with write transaction before rolling back.
            verifyAfterDeletions(
                conn->getActiveTransaction(), IDIndex, NUM_KEYS_FROM_CSV, false /* assert false */);
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        // Lookup with read transaction.
        readConn->beginReadOnlyTransaction();
        if (isCommit) {
            verifyAfterDeletions(readConn->getActiveTransaction(), IDIndex, NUM_KEYS_FROM_CSV,
                false /* assert false */);
            verifyAfterDeletions(
                conn->getActiveTransaction(), IDIndex, NUM_KEYS_FROM_CSV, false /* assert false */);
        } else {
            verifyAfterDeletions(readConn->getActiveTransaction(), IDIndex, NUM_KEYS_FROM_CSV,
                true /* assert true */);
            verifyAfterDeletions(
                conn->getActiveTransaction(), IDIndex, NUM_KEYS_FROM_CSV, true /* assert true */);
        }
        readConn->commit();
        conn->commit();
    }

    void mixedDeleteAndInsertTest(bool isCommit, TransactionTestType transactionTestType) {
        auto numKeysToInsert = 1000;
        conn->beginWriteTransaction();
        for (auto i = 0u; i < numKeysToInsert; i++) {
            uint64_t key = NUM_KEYS_FROM_CSV + i;
            ASSERT_TRUE(IDIndex->insert(conn->getActiveTransaction(), key, key << 1));
        }
        for (auto i = 0u; i < (NUM_KEYS_FROM_CSV + numKeysToInsert); i++) {
            if (i % 2 == 0) {
                IDIndex->deleteKey(conn->getActiveTransaction(), i);
            }
        }
        if (isCommit) {
            // Lookup with read transaction before committing.
            readConn->beginReadOnlyTransaction();
            verifyAfterInsertions(readConn->getActiveTransaction(), IDIndex, numKeysToInsert,
                false /* assert false */);
            verifyAfterDeletions(readConn->getActiveTransaction(), IDIndex, NUM_KEYS_FROM_CSV,
                true /* assert true */);
            readConn->commit();
        } else {
            // Lookup with write transaction before rolling back.
            verifyAfterDeletions(conn->getActiveTransaction(), IDIndex,
                (NUM_KEYS_FROM_CSV + numKeysToInsert), false /* assert false */);
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        // Lookup with read transaction.
        readConn->beginReadOnlyTransaction();
        if (isCommit) {
            verifyAfterDeletions(readConn->getActiveTransaction(), IDIndex,
                (NUM_KEYS_FROM_CSV + numKeysToInsert), false /* assert false */);
            verifyAfterDeletions(conn->getActiveTransaction(), IDIndex,
                (NUM_KEYS_FROM_CSV + numKeysToInsert), false /* assert false */);
        } else {
            verifyAfterDeletions(readConn->getActiveTransaction(), IDIndex, NUM_KEYS_FROM_CSV,
                true /* assert true */);
            verifyAfterInsertions(
                conn->getActiveTransaction(), IDIndex, numKeysToInsert, false /* assert false */);
        }
        readConn->commit();
        conn->commit();
    }

public:
    unique_ptr<Connection> readConn;
    HashIndex* IDIndex;
};

TEST_F(HashIndexTest, HashIndexBasicLookup) {
    node_offset_t result;
    readConn->beginReadOnlyTransaction();
    conn->beginWriteTransaction();
    for (auto i = 0u; i < NUM_KEYS_FROM_CSV; i++) {
        ASSERT_TRUE(IDIndex->lookup(readConn->getActiveTransaction(), i, result));
        ASSERT_EQ(result, i);
    }
    for (int64_t i = NUM_KEYS_FROM_CSV; i < (2 * NUM_KEYS_FROM_CSV); i++) {
        ASSERT_FALSE(IDIndex->lookup(conn->getActiveTransaction(), i, result));
    }
    readConn->commit();
    conn->commit();
}

TEST_F(HashIndexTest, HashIndexBasicDuplicateInsert) {
    conn->beginWriteTransaction();
    for (auto i = 0u; i < 100; i++) {
        uint64_t key = NUM_KEYS_FROM_CSV + i;
        ASSERT_TRUE(IDIndex->insert(conn->getActiveTransaction(), key, key << 1));
    }
    conn->commit();
    conn->beginWriteTransaction();
    for (auto i = 0u; i < 100; i++) {
        uint64_t key = NUM_KEYS_FROM_CSV + i;
        ASSERT_FALSE(IDIndex->insert(conn->getActiveTransaction(), key, key));
    }
    conn->commit();
}

TEST_F(HashIndexTest, InsertCommitNormalExecution) {
    insertTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(HashIndexTest, InsertRollbackNormalExecution) {
    insertTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(HashIndexTest, InsertCommitRecovery) {
    insertTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(HashIndexTest, InsertRollbackRecovery) {
    insertTest(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(HashIndexTest, DeleteCommitNormalExecution) {
    deleteTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(HashIndexTest, DeleteRollbackNormalExecution) {
    deleteTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(HashIndexTest, DeleteCommitRecovery) {
    deleteTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(HashIndexTest, DeleteRollbackRecovery) {
    deleteTest(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(HashIndexTest, MixedDeleteAndInsertCommitNormalExecution) {
    mixedDeleteAndInsertTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(HashIndexTest, MixedDeleteAndInsertRollbackNormalExecution) {
    mixedDeleteAndInsertTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(HashIndexTest, MixedDeleteAndInsertCommitRecovery) {
    mixedDeleteAndInsertTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(HashIndexTest, MixedDeleteAndInsertRollbackRecovery) {
    mixedDeleteAndInsertTest(false /* rollback */, TransactionTestType::RECOVERY);
}
