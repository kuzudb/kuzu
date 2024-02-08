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

void DBTest::createNewDB() {
    database.reset();
    conn.reset();
    removeDir(databasePath);
    createDBAndConn();
}

void DBTest::importDB(std::string filePath) {
    TestHelper::executeImportDBScript(filePath + "/" + TestHelper::SCHEMA_FILE_NAME, *conn);
    TestHelper::executeImportDBScript(filePath + "/" + TestHelper::COPY_FILE_NAME, *conn);
    TestHelper::executeImportDBScript(filePath + "/macro.cypher", *conn);
}

} // namespace testing
} // namespace kuzu
