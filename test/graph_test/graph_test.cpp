#include "graph_test/graph_test.h"

#include "binder/binder.h"
#include "common/string_format.h"
#include "parser/parser.h"
#include "spdlog/spdlog.h"
#include "storage/storage_manager.h"
#include "transaction/transaction_context.h"

using ::testing::Test;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace testing {

void PrivateGraphTest::validateColumnFilesExistence(
    std::string fileName, bool existence, bool hasOverflow) {
    ASSERT_EQ(FileUtils::fileOrPathExists(fileName), existence);
    if (hasOverflow) {
        ASSERT_EQ(
            FileUtils::fileOrPathExists(StorageUtils::getOverflowFileName(fileName)), existence);
    }
}

void PrivateGraphTest::validateListFilesExistence(
    std::string fileName, bool existence, bool hasOverflow, bool hasHeader) {
    ASSERT_EQ(FileUtils::fileOrPathExists(fileName), existence);
    ASSERT_EQ(FileUtils::fileOrPathExists(StorageUtils::getListMetadataFName(fileName)), existence);
    if (hasOverflow) {
        ASSERT_EQ(
            FileUtils::fileOrPathExists(StorageUtils::getOverflowFileName(fileName)), existence);
    }
    if (hasHeader) {
        ASSERT_EQ(
            FileUtils::fileOrPathExists(StorageUtils::getListHeadersFName(fileName)), existence);
    }
}

void PrivateGraphTest::validateNodeColumnFilesExistence(
    NodeTableSchema* nodeTableSchema, DBFileType dbFileType, bool existence) {
    validateColumnFilesExistence(
        StorageUtils::getNodeIndexFName(databasePath, nodeTableSchema->tableID, dbFileType),
        existence,
        containsOverflowFile(nodeTableSchema->getPrimaryKey()->getDataType()->getLogicalTypeID()));
}

void PrivateGraphTest::validateRelColumnAndListFilesExistence(
    RelTableSchema* relTableSchema, DBFileType dbFileType, bool existence) {
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        if (relTableSchema->getRelMultiplicity() == RelMultiplicity::MANY_ONE ||
            relTableSchema->getRelMultiplicity() == RelMultiplicity::ONE_ONE) {
            validateColumnFilesExistence(StorageUtils::getAdjColumnFName(databasePath,
                                             relTableSchema->tableID, relDirection, dbFileType),
                existence, false /* hasOverflow */);
            validateRelPropertyFiles(
                relTableSchema, relDirection, true /* isColumnProperty */, dbFileType, existence);

        } else {
            validateListFilesExistence(StorageUtils::getAdjListsFName(databasePath,
                                           relTableSchema->tableID, relDirection, dbFileType),
                existence, false /* hasOverflow */, true /* hasHeader */);
            validateRelPropertyFiles(
                relTableSchema, relDirection, false /* isColumnProperty */, dbFileType, existence);
        }
    }
}

void PrivateGraphTest::validateQueryBestPlanJoinOrder(
    std::string query, std::string expectedJoinOrder) {
    auto catalog = getCatalog(*database);
    auto statement = parser::Parser::parseQuery(query);
    auto parsedQuery = (parser::RegularQuery*)statement.get();
    auto boundQuery = Binder(*catalog, database->memoryManager.get(),
        database->storageManager.get(), conn->clientContext.get())
                          .bind(*parsedQuery);
    auto plan = Planner::getBestPlan(*catalog,
        *getStorageManager(*database)->getNodesStore().getNodesStatisticsAndDeletedIDs(),
        *getStorageManager(*database)->getRelsStore().getRelsStatistics(), *boundQuery);
    ASSERT_STREQ(LogicalPlanUtil::encodeJoin(*plan).c_str(), expectedJoinOrder.c_str());
}

void PrivateGraphTest::validateRelPropertyFiles(catalog::RelTableSchema* relTableSchema,
    RelDataDirection relDirection, bool isColumnProperty, DBFileType dbFileType, bool existence) {
    for (auto& property : relTableSchema->properties) {
        auto hasOverflow = containsOverflowFile(property->getDataType()->getLogicalTypeID());
        if (isColumnProperty) {
            validateColumnFilesExistence(
                StorageUtils::getRelPropertyColumnFName(databasePath, relTableSchema->tableID,
                    relDirection, property->getPropertyID(), dbFileType),
                existence, hasOverflow);
        } else {
            validateListFilesExistence(
                StorageUtils::getRelPropertyListsFName(databasePath, relTableSchema->tableID,
                    relDirection, property->getPropertyID(), dbFileType),
                existence, hasOverflow, false /* hasHeader */);
        }
    }
}

void DBTest::createDB(uint64_t checkpointWaitTimeout) {
    if (database != nullptr) {
        database.reset();
    }
    database = std::make_unique<main::Database>(databasePath, *systemConfig);
    getTransactionManager(*database)->setCheckPointWaitTimeoutForTransactionsToLeaveInMicros(
        checkpointWaitTimeout /* 10ms */);
    spdlog::set_level(spdlog::level::info);
}

} // namespace testing
} // namespace kuzu
