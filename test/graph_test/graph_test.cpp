#include "graph_test/graph_test.h"

using ::testing::Test;

namespace kuzu {
namespace testing {

void BaseGraphTest::validateColumnFilesExistence(
    string fileName, bool existence, bool hasOverflow) {
    ASSERT_EQ(FileUtils::fileOrPathExists(fileName), existence);
    if (hasOverflow) {
        ASSERT_EQ(
            FileUtils::fileOrPathExists(StorageUtils::getOverflowFileName(fileName)), existence);
    }
}

void BaseGraphTest::validateListFilesExistence(
    string fileName, bool existence, bool hasOverflow, bool hasHeader) {
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
        unordered_set<table_id_t> boundTableIDs = relDirection == FWD ?
                                                      relTableSchema->getUniqueSrcTableIDs() :
                                                      relTableSchema->getUniqueDstTableIDs();
        if (relTableSchema->relMultiplicity) {
            for (auto boundTableID : boundTableIDs) {
                validateColumnFilesExistence(
                    StorageUtils::getAdjColumnFName(databaseConfig->databasePath,
                        relTableSchema->tableID, boundTableID, relDirection, dbFileType),
                    existence, false /* hasOverflow */);
                validateRelPropertyFiles(relTableSchema, boundTableID, relDirection,
                    true /* isColumnProperty */, dbFileType, existence);
            }
        } else {
            for (auto boundTableID : boundTableIDs) {
                validateListFilesExistence(
                    StorageUtils::getAdjListsFName(databaseConfig->databasePath,
                        relTableSchema->tableID, boundTableID, relDirection, dbFileType),
                    existence, false /* hasOverflow */, true /* hasHeader */);
                validateRelPropertyFiles(relTableSchema, boundTableID, relDirection,
                    false /* isColumnProperty */, dbFileType, existence);
            }
        }
    }
}

void BaseGraphTest::validateQueryBestPlanJoinOrder(string query, string expectedJoinOrder) {
    auto catalog = getCatalog(*database);
    auto statement = Parser::parseQuery(query);
    auto parsedQuery = (RegularQuery*)statement.get();
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
    table_id_t tableID, RelDirection relDirection, bool isColumnProperty, DBFileType dbFileType,
    bool existence) {
    for (auto& property : relTableSchema->properties) {
        auto hasOverflow = containsOverflowFile(property.dataType.typeID);
        if (isColumnProperty) {
            validateColumnFilesExistence(
                StorageUtils::getRelPropertyColumnFName(databaseConfig->databasePath,
                    relTableSchema->tableID, tableID, relDirection, property.propertyID,
                    dbFileType),
                existence, hasOverflow);
        } else {
            validateListFilesExistence(StorageUtils::getRelPropertyListsFName(
                                           databaseConfig->databasePath, relTableSchema->tableID,
                                           tableID, relDirection, property.propertyID, dbFileType),
                existence, hasOverflow, false /* hasHeader */);
        }
    }
}

void TestHelper::executeCypherScript(const string& cypherScript, Connection& conn) {
    cout << "cypherScript: " << cypherScript << endl;
    assert(FileUtils::fileOrPathExists(cypherScript));
    ifstream file(cypherScript);
    if (!file.is_open()) {
        throw Exception(StringUtils::string_format(
            "Error opening file: %s, errno: %d.", cypherScript.c_str(), errno));
    }
    string line;
    while (getline(file, line)) {
        // If this is a COPY_CSV statement, we need to append the KUZU_ROOT_DIRECTORY to the csv
        // file path.
        auto pos = line.find('"');
        if (pos != string::npos) {
            line.insert(pos + 1, KUZU_ROOT_DIRECTORY + string("/"));
        }
        cout << "Starting to execute query: " << line << endl;
        auto result = conn.query(line);
        cout << "Executed query: " << line << endl;
        if (!result->isSuccess()) {
            throw Exception(
                StringUtils::string_format("Failed to execute statement: %s.\nError: %s",
                    line.c_str(), result->getErrorMessage().c_str()));
        }
    }
}

void BaseGraphTest::initGraph() {
    TestHelper::executeCypherScript(getInputDir() + TestHelper::SCHEMA_FILE_NAME, *conn);
    TestHelper::executeCypherScript(getInputDir() + TestHelper::COPY_CSV_FILE_NAME, *conn);
}

void BaseGraphTest::initGraphFromPath(const string& path) const {
    TestHelper::executeCypherScript(path + TestHelper::SCHEMA_FILE_NAME, *conn);
    TestHelper::executeCypherScript(path + TestHelper::COPY_CSV_FILE_NAME, *conn);
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