#include "test_helper/test_helper.h"

#include <fstream>

#include "spdlog/spdlog.h"

using namespace std;
using namespace kuzu::planner;

namespace kuzu {
namespace testing {

vector<unique_ptr<TestQueryConfig>> TestHelper::parseTestFile(
    const string& path, bool checkOutputOrder) {
    vector<unique_ptr<TestQueryConfig>> result;
    if (access(path.c_str(), 0) != 0) {
        throw Exception("Test file not exists! [" + path + "].");
    }
    struct stat status {};
    stat(path.c_str(), &status);
    if (status.st_mode & S_IFDIR) {
        throw Exception("Test file is a directory. [" + path + "].");
    }
    ifstream ifs(path);
    string line;
    TestQueryConfig* currentConfig;
    while (getline(ifs, line)) {
        if (line.starts_with("-NAME")) {
            auto config = make_unique<TestQueryConfig>();
            currentConfig = config.get();
            result.push_back(move(config));
            currentConfig->name = line.substr(6, line.length());
        } else if (line.starts_with("-QUERY")) {
            currentConfig->query = line.substr(7, line.length());
        } else if (line.starts_with("-PARALLELISM")) {
            currentConfig->numThreads = stoi(line.substr(13, line.length()));
        } else if (line.starts_with("-ENCODED_JOIN")) {
            currentConfig->encodedJoin = line.substr(14, line.length());
        } else if (line.starts_with("-ENUMERATE")) {
            currentConfig->enumerate = true;
        } else if (line.starts_with("----")) {
            uint64_t numTuples = stoi(line.substr(5, line.length()));
            currentConfig->expectedNumTuples = numTuples;
            for (auto i = 0u; i < numTuples; i++) {
                getline(ifs, line);
                currentConfig->expectedTuples.push_back(line);
            }
            if (!checkOutputOrder) { // order is not important for result
                sort(currentConfig->expectedTuples.begin(), currentConfig->expectedTuples.end());
            }
        }
    }
    return result;
}

bool TestHelper::testQueries(vector<unique_ptr<TestQueryConfig>>& configs, Connection& conn) {
    spdlog::set_level(spdlog::level::info);
    auto numPassedQueries = 0u;
    vector<uint64_t> failedQueries;
    for (auto i = 0u; i < configs.size(); i++) {
        if (testQuery(configs[i].get(), conn)) {
            numPassedQueries++;
        } else {
            failedQueries.push_back(i);
        }
    }
    spdlog::info("SUMMARY:");
    if (failedQueries.empty()) {
        spdlog::info("ALL QUERIES PASSED.");
    } else {
        for (auto& idx : failedQueries) {
            spdlog::info("QUERY {} NOT PASSED.", configs[idx]->name);
        }
    }
    return numPassedQueries == configs.size();
}

vector<string> TestHelper::convertResultToString(QueryResult& queryResult, bool checkOutputOrder) {
    vector<string> actualOutput;
    while (queryResult.hasNext()) {
        auto tuple = queryResult.getNext();
        actualOutput.push_back(tuple->toString(vector<uint32_t>(tuple->len(), 0)));
    }
    if (!checkOutputOrder) {
        sort(actualOutput.begin(), actualOutput.end());
    }
    return actualOutput;
}

void TestHelper::initializeConnection(TestQueryConfig* config, Connection& conn) {
    spdlog::info("TEST: {}", config->name);
    spdlog::info("QUERY: {}", config->query);
    conn.setMaxNumThreadForExec(config->numThreads);
}

bool TestHelper::testQuery(TestQueryConfig* config, Connection& conn) {
    initializeConnection(config, conn);
    vector<unique_ptr<LogicalPlan>> plans;
    if (config->enumerate) {
        plans = conn.enumeratePlans(config->query);
    } else {
        plans.push_back(conn.getBestPlan(config->query));
    }
    assert(!plans.empty());
    auto numPassedPlans = 0u;
    for (auto i = 0u; i < plans.size(); ++i) {
        auto planStr = plans[i]->toString();
        auto result = conn.executePlan(std::move(plans[i]));
        assert(result->isSuccess());
        vector<string> resultTuples = convertResultToString(*result, config->checkOutputOrder);
        if (resultTuples.size() == result->getNumTuples() &&
            resultTuples == config->expectedTuples) {
            spdlog::info(
                "PLAN{} PASSED in {}ms.", i, result->getQuerySummary()->getExecutionTime());
            numPassedPlans++;
        } else {
            spdlog::error("PLAN{} NOT PASSED.", i);
            spdlog::info("PLAN: \n{}", planStr);
            spdlog::info("RESULT: \n");
            for (auto& tuple : resultTuples) {
                spdlog::info(tuple);
            }
        }
    }
    spdlog::info("{}/{} plans passed.", numPassedPlans, plans.size());
    return numPassedPlans == plans.size();
}

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
    for (auto& property : nodeTableSchema->structuredProperties) {
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

void BaseGraphTest::validateRelPropertyFiles(catalog::RelTableSchema* relTableSchema,
    table_id_t tableID, RelDirection relDirection, bool isColumnProperty, DBFileType dbFileType,
    bool existence) {
    for (auto i = 0u; i < relTableSchema->getNumProperties(); ++i) {
        auto property = relTableSchema->properties[i];
        auto hasOverflow = containsOverflowFile(property.dataType.typeID);
        if (isColumnProperty) {
            validateColumnFilesExistence(
                StorageUtils::getRelPropertyColumnFName(databaseConfig->databasePath,
                    relTableSchema->tableID, tableID, relDirection, property.propertyID,
                    dbFileType),
                existence, hasOverflow);
        } else {
            validateListFilesExistence(
                StorageUtils::getRelPropertyListsFName(databaseConfig->databasePath,
                    relTableSchema->tableID, tableID, relDirection,
                    relTableSchema->properties[i].propertyID, dbFileType),
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
    TestHelper::executeCypherScript(getInputCSVDir() + TestHelper::SCHEMA_FILE_NAME, *conn);
    TestHelper::executeCypherScript(getInputCSVDir() + TestHelper::COPY_CSV_FILE_NAME, *conn);
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
