#include "graph_test/graph_test.h"

#include "binder/binder.h"
#include "parser/parser.h"
#include "planner/operator/logical_plan_util.h"
#include "planner/planner.h"
#include "spdlog/spdlog.h"
#include "storage/storage_manager.h"
#include "transaction/transaction_manager.h"

using ::testing::Test;
using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace testing {

void PrivateGraphTest::validateQueryBestPlanJoinOrder(
    std::string query, std::string expectedJoinOrder) {
    auto catalog = getCatalog(*database);
    auto statement = parser::Parser::parseQuery(query);
    auto parsedQuery = (parser::RegularQuery*)statement.get();
    auto boundQuery =
        Binder(*catalog, database->memoryManager.get(), database->storageManager.get(),
            database->vfs.get(), conn->clientContext.get(), database->extensionOptions.get())
            .bind(*parsedQuery);
    auto planner = Planner(catalog, getStorageManager(*database));
    auto plan = planner.getBestPlan(*boundQuery);
    ASSERT_STREQ(LogicalPlanUtil::encodeJoin(*plan).c_str(), expectedJoinOrder.c_str());
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

void DBTest::createNewDB(uint64_t checkpointWaitTimeout) {
    auto newDatabasePath =
        TestHelper::appendKuzuRootPath(TestHelper::TMP_TEST_DIR + getTestGroupAndName() +
                                       TestHelper::getMillisecondsSuffix() + "exportOnly");
    newDatabase = std::make_unique<main::Database>(newDatabasePath, *systemConfig);
    getTransactionManager(*newDatabase)
        ->setCheckPointWaitTimeoutForTransactionsToLeaveInMicros(checkpointWaitTimeout /* 10ms */);
    newConn = std::make_unique<main::Connection>(newDatabase.get());
}

void DBTest::importDB(std::string filePath) {
    TestHelper::executeImportDBScript(filePath + "/" + TestHelper::SCHEMA_FILE_NAME, *newConn);
    TestHelper::executeImportDBScript(filePath + "/" + TestHelper::COPY_FILE_NAME, *newConn);
}

} // namespace testing
} // namespace kuzu
