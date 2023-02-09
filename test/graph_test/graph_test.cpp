#include "graph_test/graph_test.h"

#include "binder/binder.h"
#include "json.hpp"
#include "parser/parser.h"
#include "storage/storage_manager.h"

using ::testing::Test;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace testing {

void BaseGraphTest::validateColumnFilesExistence(
    std::string fileName, bool existence, bool hasOverflow) {
    ASSERT_EQ(FileUtils::fileOrPathExists(fileName), existence);
    if (hasOverflow) {
        ASSERT_EQ(
            FileUtils::fileOrPathExists(StorageUtils::getOverflowFileName(fileName)), existence);
    }
}

void BaseGraphTest::validateListFilesExistence(
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

void BaseGraphTest::validateNodeColumnFilesExistence(
    NodeTableSchema* nodeTableSchema, DBFileType dbFileType, bool existence) {
    for (auto& property : nodeTableSchema->properties) {
        validateColumnFilesExistence(
            StorageUtils::getNodePropertyColumnFName(databaseConfig->databasePath,
                nodeTableSchema->tableID, property.propertyID, dbFileType),
            existence, containsOverflowFile(property.dataType.typeID));
    }
    validateColumnFilesExistence(StorageUtils::getNodeIndexFName(databaseConfig->databasePath,
                                     nodeTableSchema->tableID, dbFileType),
        existence, containsOverflowFile(nodeTableSchema->getPrimaryKey().dataType.typeID));
}

void BaseGraphTest::validateRelColumnAndListFilesExistence(
    RelTableSchema* relTableSchema, DBFileType dbFileType, bool existence) {
    for (auto relDirection : REL_DIRECTIONS) {
        if (relTableSchema->relMultiplicity) {
            validateColumnFilesExistence(
                StorageUtils::getAdjColumnFName(databaseConfig->databasePath,
                    relTableSchema->tableID, relDirection, dbFileType),
                existence, false /* hasOverflow */);
            validateRelPropertyFiles(
                relTableSchema, relDirection, true /* isColumnProperty */, dbFileType, existence);

        } else {
            validateListFilesExistence(StorageUtils::getAdjListsFName(databaseConfig->databasePath,
                                           relTableSchema->tableID, relDirection, dbFileType),
                existence, false /* hasOverflow */, true /* hasHeader */);
            validateRelPropertyFiles(
                relTableSchema, relDirection, false /* isColumnProperty */, dbFileType, existence);
        }
    }
}

void BaseGraphTest::validateQueryBestPlanJoinOrder(
    std::string query, std::string expectedJoinOrder) {
    auto catalog = getCatalog(*database);
    auto statement = parser::Parser::parseQuery(query);
    auto parsedQuery = (parser::RegularQuery*)statement.get();
    auto boundQuery = Binder(*catalog).bind(*parsedQuery);
    auto plan = Planner::getBestPlan(*catalog,
        getStorageManager(*database)->getNodesStore().getNodesStatisticsAndDeletedIDs(),
        getStorageManager(*database)->getRelsStore().getRelsStatistics(), *boundQuery);
    ASSERT_STREQ(LogicalPlanUtil::encodeJoin(*plan).c_str(), expectedJoinOrder.c_str());
}

void BaseGraphTest::commitOrRollbackConnectionAndInitDBIfNecessary(
    bool isCommit, TransactionTestType transactionTestType) {
    commitOrRollbackConnection(isCommit, transactionTestType);
    if (transactionTestType == TransactionTestType::RECOVERY) {
        // This creates a new database/conn/readConn and should run the recovery algorithm.
        createDBAndConn();
        conn->beginWriteTransaction();
    }
}

void BaseGraphTest::validateRelPropertyFiles(catalog::RelTableSchema* relTableSchema,
    RelDirection relDirection, bool isColumnProperty, DBFileType dbFileType, bool existence) {
    for (auto& property : relTableSchema->properties) {
        auto hasOverflow = containsOverflowFile(property.dataType.typeID);
        if (isColumnProperty) {
            validateColumnFilesExistence(
                StorageUtils::getRelPropertyColumnFName(databaseConfig->databasePath,
                    relTableSchema->tableID, relDirection, property.propertyID, dbFileType),
                existence, hasOverflow);
        } else {
            validateListFilesExistence(
                StorageUtils::getRelPropertyListsFName(databaseConfig->databasePath,
                    relTableSchema->tableID, relDirection, property.propertyID, dbFileType),
                existence, hasOverflow, false /* hasHeader */);
        }
    }
}

void TestHelper::executeScript(const std::string& cypherScript, Connection& conn) {
    std::cout << "cypherScript: " << cypherScript << std::endl;
    assert(FileUtils::fileOrPathExists(cypherScript));
    std::ifstream file(cypherScript);
    if (!file.is_open()) {
        throw Exception(StringUtils::string_format(
            "Error opening file: %s, errno: %d.", cypherScript.c_str(), errno));
    }
    std::string line;
    while (getline(file, line)) {
        // If this is a COPY_CSV statement, we need to append the KUZU_ROOT_DIRECTORY to the csv
        // file path.
        auto pos = line.find('"');
        if (pos != std::string::npos) {
            line.insert(pos + 1, KUZU_ROOT_DIRECTORY + std::string("/"));
        }
        std::cout << "Starting to execute query: " << line << std::endl;
        auto result = conn.query(line);
        std::cout << "Executed query: " << line << std::endl;
        if (!result->isSuccess()) {
            throw Exception(
                StringUtils::string_format("Failed to execute statement: %s.\nError: %s",
                    line.c_str(), result->getErrorMessage().c_str()));
        }
    }
}

void BaseGraphTest::createDBAndConn() {
    if (database != nullptr) {
        database.reset();
    }
    database = std::make_unique<main::Database>(*databaseConfig, *systemConfig);
    conn = std::make_unique<main::Connection>(database.get());
    spdlog::set_level(spdlog::level::info);
}

void BaseGraphTest::initGraph() {
    TestHelper::executeScript(getInputDir() + TestHelper::SCHEMA_FILE_NAME, *conn);
    TestHelper::executeScript(getInputDir() + TestHelper::COPY_FILE_NAME, *conn);
}

void BaseGraphTest::commitOrRollbackConnection(
    bool isCommit, TransactionTestType transactionTestType) const {
    if (transactionTestType == TransactionTestType::NORMAL_EXECUTION) {
        if (isCommit) {
            conn->commit();
        } else {
            conn->rollback();
        }
        conn->beginWriteTransaction();
    } else {
        if (isCommit) {
            conn->commitButSkipCheckpointingForTestingRecovery();
        } else {
            conn->rollbackButSkipCheckpointingForTestingRecovery();
        }
    }
}

} // namespace testing
} // namespace kuzu