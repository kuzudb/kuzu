#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

namespace graphflow {
namespace transaction {

class TinySnbCopyCSVTest : public BaseGraphLoadingTest {

public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        createDBAndConn();
        catalog = conn->database->getCatalog();
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = conn->database->getCatalog();
    }

    void validateNodeColumnAndListFilesExist(label_t labelID, bool isForWalRecord) {
        for (auto& property :
            catalog->getReadOnlyVersion()->getNodeLabel(labelID)->structuredProperties) {
            auto fName =
                StorageUtils::getNodePropertyColumnFName(conn->database->getWAL()->getDirectory(),
                    labelID, property.propertyID, isForWalRecord);
            ASSERT_EQ(FileUtils::fileExists(fName), true);
        }
        auto unstructuredPropertyListsFile = StorageUtils::getNodeUnstrPropertyListsFName(
            conn->database->getWAL()->getDirectory(), labelID, isForWalRecord);
        ASSERT_EQ(FileUtils::fileExists(unstructuredPropertyListsFile), true);
    }

    void copyNodeCSVCommitAndRecoveryTest(bool testRecovery) {
        auto result = conn->query(CREATE_UNIVERSITY_TABLE_CMD);
        auto preparedStatement = conn->prepareNoLock(COPY_UNIVERSITY_CSV_CMD);
        conn->beginTransactionNoLock(WRITE);
        auto profiler = make_unique<Profiler>();
        auto executionContext = make_unique<ExecutionContext>(1, profiler.get(), nullptr, nullptr);
        database->queryProcessor->execute(
            preparedStatement->physicalPlan.get(), executionContext.get());
        auto labelID = catalog->getReadOnlyVersion()->getNodeLabelFromName("UNIVERSITY");
        validateNodeColumnAndListFilesExist(labelID, true /* isForWALRecord */);
        if (testRecovery) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            ASSERT_EQ(
                database->getStorageManager()->getNodesStore().getNodesMetadata().getMaxNodeOffset(
                    TransactionType::READ_ONLY, labelID),
                UINT64_MAX);
            validateNodeColumnAndListFilesExist(labelID, true /* isForWALRecord */);
            initWithoutLoadingGraph();
            validateNodeColumnAndListFilesExist(labelID, false /* isForWALRecord */);
            ASSERT_EQ(
                database->getStorageManager()->getNodesStore().getNodesMetadata().getMaxNodeOffset(
                    TransactionType::READ_ONLY, labelID),
                2);
        } else {
            conn->commit();
            validateNodeColumnAndListFilesExist(labelID, false /* isForWALRecord */);
            ASSERT_EQ(
                database->getStorageManager()->getNodesStore().getNodesMetadata().getMaxNodeOffset(
                    TransactionType::READ_ONLY, labelID),
                2);
        }
    }
    static constexpr char CREATE_UNIVERSITY_TABLE_CMD[] =
        "CREATE NODE TABLE UNIVERSITY(ID INT64, history INT64)";
    static constexpr char COPY_UNIVERSITY_CSV_CMD[] =
        "COPY UNIVERSITY FROM \"dataset/copy_csv/university.csv\"";
    Catalog* catalog;
};
} // namespace transaction
} // namespace graphflow

TEST_F(TinySnbCopyCSVTest, CopyNodeCSVTest) {
    auto result = conn->query(CREATE_UNIVERSITY_TABLE_CMD);
    result = conn->query(COPY_UNIVERSITY_CSV_CMD);
    ASSERT_EQ(result->isSuccess(), true);
    result = conn->query("match (u:UNIVERSITY) return u.history");
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->val.int64Val, 5);
    tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->val.int64Val, 2);
    tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->val.int64Val, 4);
}

TEST_F(TinySnbCopyCSVTest, CopyNodeCSVCommitTest) {
    copyNodeCSVCommitAndRecoveryTest(false /* testRecovery */);
}

TEST_F(TinySnbCopyCSVTest, CopyNodeCSVCommitRecoveryTest) {
    copyNodeCSVCommitAndRecoveryTest(true /* testRecovery */);
}
