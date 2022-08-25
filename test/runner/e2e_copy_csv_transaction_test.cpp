#include <set>

#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

namespace graphflow {
namespace transaction {

class TinySnbCopyCSVTransactionTest : public EmptyDBTest {

public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
        catalog = conn->database->getCatalog();
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = conn->database->getCatalog();
    }

    static inline bool containsOverflowFile(DataTypeID typeID) {
        return typeID == STRING || typeID == LIST || typeID == UNSTRUCTURED;
    }

    void validateColumnFilesExistence(string fileName, bool existence, bool hasOverflow) {
        ASSERT_EQ(FileUtils::fileOrPathExists(fileName), existence);
        if (hasOverflow) {
            ASSERT_EQ(FileUtils::fileOrPathExists(StorageUtils::getOverflowPagesFName(fileName)),
                existence);
        }
    }

    void validateListFilesExistence(
        string fileName, bool existence, bool hasOverflow, bool hasHeader) {
        ASSERT_EQ(FileUtils::fileOrPathExists(fileName), existence);
        ASSERT_EQ(
            FileUtils::fileOrPathExists(StorageUtils::getListMetadataFName(fileName)), existence);
        if (hasOverflow) {
            ASSERT_EQ(FileUtils::fileOrPathExists(StorageUtils::getOverflowPagesFName(fileName)),
                existence);
        }
        if (hasHeader) {
            ASSERT_EQ(FileUtils::fileOrPathExists(StorageUtils::getListHeadersFName(fileName)),
                existence);
        }
    }

    void validateNodeColumnAndListFilesExistence(
        label_t labelID, DBFileType dbFileType, bool existence) {
        auto nodeLabel = catalog->getReadOnlyVersion()->getNodeLabel(labelID);
        for (auto& property : nodeLabel->structuredProperties) {
            validateColumnFilesExistence(
                StorageUtils::getNodePropertyColumnFName(
                    databaseConfig->databasePath, labelID, property.propertyID, dbFileType),
                existence, containsOverflowFile(property.dataType.typeID));
        }
        validateListFilesExistence(StorageUtils::getNodeUnstrPropertyListsFName(
                                       databaseConfig->databasePath, labelID, dbFileType),
            existence, true /* hasOverflow */, true /* hasHeader */);
        validateColumnFilesExistence(
            StorageUtils::getNodeIndexFName(databaseConfig->databasePath, labelID, dbFileType),
            existence, containsOverflowFile(nodeLabel->getPrimaryKey().dataType.typeID));
    }

    void validateTinysnbPersonAgeProperty() {
        multiset<int, greater<>> expectedResult = {20, 20, 25, 30, 35, 40, 45, 83};
        multiset<int, greater<>> actualResult;
        auto queryResult = conn->query("match (p:person) return p.age");
        while (queryResult->hasNext()) {
            auto tuple = queryResult->getNext();
            actualResult.insert(tuple->getValue(0)->val.int64Val);
        }
        ASSERT_EQ(expectedResult, actualResult);
    }

    void validateDatabaseStateBeforeCheckPointCopyNodeCSV(label_t labelID) {
        // Before checkPointing, we should have two versions of node column and list files. The
        // updates to maxNodeOffset should be invisible to read-only transactions.
        validateNodeColumnAndListFilesExistence(
            labelID, DBFileType::WAL_VERSION, true /* existence */);
        validateNodeColumnAndListFilesExistence(
            labelID, DBFileType::ORIGINAL, true /* existence */);
        ASSERT_EQ(make_unique<Connection>(database.get())
                      ->query("MATCH (p:person) return *")
                      ->getNumTuples(),
            0);
        ASSERT_EQ(database->storageManager->getNodesStore().getNodesMetadata().getMaxNodeOffset(
                      TransactionType::READ_ONLY, labelID),
            UINT64_MAX);
    }

    void validateDatabaseStateAfterCheckPointCopyNodeCSV(label_t labelID) {
        // After checkPointing, we should only have one version of node column and list
        // files(original version). The updates to maxNodeOffset should be visible to read-only
        // transaction;
        validateNodeColumnAndListFilesExistence(
            labelID, DBFileType::WAL_VERSION, false /* existence */);
        validateNodeColumnAndListFilesExistence(
            labelID, DBFileType::ORIGINAL, true /* existence */);
        validateTinysnbPersonAgeProperty();
        ASSERT_EQ(
            database->getStorageManager()->getNodesStore().getNodesMetadata().getMaxNodeOffset(
                TransactionType::READ_ONLY, labelID),
            7);
    }

    void copyNodeCSVCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query(createPersonTableCmd);
        auto preparedStatement = conn->prepareNoLock(copyPersonTableCmd);
        conn->beginTransactionNoLock(WRITE);
        database->queryProcessor->execute(
            preparedStatement->physicalPlan.get(), nullptr /* executionContext */);
        auto labelID = catalog->getReadOnlyVersion()->getNodeLabelFromName("person");
        validateDatabaseStateBeforeCheckPointCopyNodeCSV(labelID);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            validateDatabaseStateBeforeCheckPointCopyNodeCSV(labelID);
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCheckPointCopyNodeCSV(labelID);
        } else {
            conn->commit();
            validateDatabaseStateAfterCheckPointCopyNodeCSV(labelID);
        }
    }

    void validateTinysnbKnowsDateProperty() {
        multiset<date_t, greater<>> expectedResult = {
            Date::FromCString("1905-12-12", strlen("1905-12-12")),
            Date::FromCString("1905-12-12", strlen("1905-12-12")),
            Date::FromCString("1950-05-14", strlen("1950-05-14")),
            Date::FromCString("1950-05-14", strlen("1950-05-14")),
            Date::FromCString("1950-05-14", strlen("1950-05-14")),
            Date::FromCString("1950-05-14", strlen("1950-05-14")),
            Date::FromCString("2000-01-01", strlen("2000-01-01")),
            Date::FromCString("2000-01-01", strlen("2000-01-01")),
            Date::FromCString("2021-06-30", strlen("2021-06-30")),
            Date::FromCString("2021-06-30", strlen("2021-06-30")),
            Date::FromCString("2021-06-30", strlen("2021-06-30")),
            Date::FromCString("2021-06-30", strlen("2021-06-30")),
            Date::FromCString("2021-06-30", strlen("2021-06-30")),
            Date::FromCString("2021-06-30", strlen("2021-06-30"))};
        multiset<date_t, greater<>> actualResult;
        auto queryResult = conn->query("match (:person)-[e:knows]->(:person) return e.date");
        while (queryResult->hasNext()) {
            auto tuple = queryResult->getNext();
            actualResult.insert(tuple->getValue(0)->val.dateVal);
        }
        ASSERT_EQ(expectedResult, actualResult);
    }

    void validateRelPropertyFiles(catalog::RelLabel* relLabel, label_t nodeLabel,
        RelDirection relDirection, bool isColumnProperty, DBFileType dbFileType, bool existence) {
        for (auto i = 0u; i < relLabel->getNumProperties(); ++i) {
            auto property = relLabel->properties[i];
            auto hasOverflow = containsOverflowFile(property.dataType.typeID);
            if (isColumnProperty) {
                validateColumnFilesExistence(
                    StorageUtils::getRelPropertyColumnFName(databaseConfig->databasePath,
                        relLabel->labelID, nodeLabel, relDirection, property.name, dbFileType),
                    existence, hasOverflow);
            } else {
                validateListFilesExistence(
                    StorageUtils::getRelPropertyListsFName(databaseConfig->databasePath,
                        relLabel->labelID, nodeLabel, relDirection,
                        relLabel->properties[i].propertyID, dbFileType),
                    existence, hasOverflow, false /* hasHeader */);
            }
        }
    }

    void validateRelColumnAndListFilesExistence(
        label_t labelID, DBFileType dbFileType, bool existence) {
        auto relLabel = catalog->getReadOnlyVersion()->getRelLabel(labelID);
        for (auto relDirection : REL_DIRECTIONS) {
            auto nodeLabels = catalog->getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(
                labelID, relDirection);
            if (catalog->getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    labelID, relDirection)) {
                for (auto nodeLabel : nodeLabels) {
                    validateColumnFilesExistence(
                        StorageUtils::getAdjColumnFName(databaseConfig->databasePath, labelID,
                            nodeLabel, relDirection, dbFileType),
                        existence, false /* hasOverflow */);
                    validateRelPropertyFiles(relLabel, nodeLabel, relDirection,
                        true /* isColumnProperty */, dbFileType, existence);
                }
            } else {
                for (auto nodeLabel : nodeLabels) {
                    validateListFilesExistence(
                        StorageUtils::getAdjListsFName(databaseConfig->databasePath, labelID,
                            nodeLabel, relDirection, dbFileType),
                        existence, false /* hasOverflow */, true /* hasHeader */);
                    validateRelPropertyFiles(relLabel, nodeLabel, relDirection,
                        false /* isColumnProperty */, dbFileType, existence);
                }
            }
        }
    }

    void validateDatabaseStateBeforeCheckPointCopyRelCSV(label_t labelID) {
        // Before checkPointing, we should have two versions of rel column and list files.
        validateRelColumnAndListFilesExistence(
            labelID, DBFileType::WAL_VERSION, true /* existence */);
        validateRelColumnAndListFilesExistence(labelID, DBFileType::ORIGINAL, true /* existence */);
    }

    void validateDatabaseStateAfterCheckPointCopyRelCSV(label_t labelID) {
        // After checkPointing, we should only have one version of rel column and list
        // files(original version).
        validateRelColumnAndListFilesExistence(
            labelID, DBFileType::WAL_VERSION, false /* existence */);
        validateRelColumnAndListFilesExistence(labelID, DBFileType::ORIGINAL, true /* existence */);
        validateTinysnbKnowsDateProperty();
    }

    void copyRelCSVCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query(createPersonTableCmd);
        conn->query(copyPersonTableCmd);
        conn->query("create rel knows (FROM person TO person, date DATE, meetTime TIMESTAMP, "
                    "validInterval INTERVAL, comments STRING[], MANY_MANY)");
        auto preparedStatement =
            conn->prepareNoLock("COPY knows FROM \"dataset/tinysnb/eKnows.csv\"");
        conn->beginTransactionNoLock(WRITE);
        auto profiler = make_unique<Profiler>();
        auto bufferManager = make_unique<BufferManager>();
        auto executionContext = make_unique<ExecutionContext>(
            1, profiler.get(), nullptr /* memoryManager */, bufferManager.get());
        database->queryProcessor->execute(
            preparedStatement->physicalPlan.get(), executionContext.get());
        auto labelID = catalog->getReadOnlyVersion()->getRelLabelFromName("knows");
        validateDatabaseStateBeforeCheckPointCopyRelCSV(labelID);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            validateDatabaseStateBeforeCheckPointCopyRelCSV(labelID);
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCheckPointCopyRelCSV(labelID);
        } else {
            conn->commit();
            validateDatabaseStateAfterCheckPointCopyRelCSV(labelID);
        }
    }

    Catalog* catalog;
    string createPersonTableCmd =
        "create node table person (ID INT64, fName STRING, gender INT64, isStudent BOOLEAN, "
        "isWorker BOOLEAN, "
        "age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration "
        "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], "
        "PRIMARY KEY (ID))";
    string copyPersonTableCmd = "COPY person FROM \"dataset/tinysnb/vPerson.csv\"";
};
} // namespace transaction
} // namespace graphflow

TEST_F(TinySnbCopyCSVTransactionTest, CopyNodeCSVCommitTest) {
    copyNodeCSVCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyNodeCSVCommitRecoveryTest) {
    copyNodeCSVCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyRelCSVCommitTest) {
    copyRelCSVCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyRelCSVCommitRecoveryTest) {
    copyRelCSVCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyCSVStatementWithActiveTransactionErrorTest) {
    auto re = conn->query(createPersonTableCmd);
    ASSERT_EQ(re->isSuccess(), true);
    conn->beginWriteTransaction();
    auto result = conn->query(copyPersonTableCmd);
    ASSERT_EQ(result->getErrorMessage(),
        "DDL and CopyCSV statements are automatically wrapped in a transaction and committed "
        "automatically. "
        "As such, they cannot be part of an active transaction, please commit or rollback your "
        "previous transaction and issue a ddl query without opening a transaction.");
}
