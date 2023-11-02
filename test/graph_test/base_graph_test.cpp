#include "graph_test/base_graph_test.h"

#include <fstream>

#include "common/string_format.h"
#include "spdlog/spdlog.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace testing {

void BaseGraphTest::commitOrRollbackConnectionAndInitDBIfNecessary(
    bool isCommit, TransactionTestType transactionTestType) {
    commitOrRollbackConnection(isCommit, transactionTestType);
    if (transactionTestType == TransactionTestType::RECOVERY) {
        // This creates a new database/conn/readConn and should run the recovery algorithm.
        createDBAndConn();
        conn->query("BEGIN TRANSACTION");
    }
}

void TestHelper::executeScript(const std::string& cypherScript, Connection& conn) {
    std::cout << "cypherScript: " << cypherScript << std::endl;
    if (!FileUtils::fileOrPathExists(cypherScript)) {
        std::cout << "CpyherScript: " << cypherScript << " doesn't exist. Skipping..." << std::endl;
        return;
    }
    std::ifstream file(cypherScript);
    if (!file.is_open()) {
        throw Exception(stringFormat("Error opening file: {}, errno: {}.", cypherScript, errno));
    }
    std::string line;
    while (getline(file, line)) {
        // If this is a COPY statement, we need to append the KUZU_ROOT_DIRECTORY to the csv
        // file path. There maybe multiple csv files in the line, so we need to find all of them.
        std::vector<std::string> csvFilePaths;
        size_t index = 0;
        while (true) {
            size_t start = line.find('"', index);
            if (start == std::string::npos) {
                break;
            }
            size_t end = line.find('"', start + 1);
            if (end == std::string::npos) {
                // No matching double quote, must not be a file path.
                break;
            }
            std::string substr = line.substr(start + 1, end - start - 1);
            // convert to lower case
            auto substrLower = substr;
            std::transform(substrLower.begin(), substrLower.end(), substrLower.begin(), ::tolower);
            if (substrLower.find(".csv") != std::string::npos ||
                substrLower.find(".parquet") != std::string::npos ||
                substrLower.find(".npy") != std::string::npos ||
                substrLower.find(".ttl") != std::string::npos) {
                csvFilePaths.push_back(substr);
            }
            index = end + 1;
        }
        for (auto& csvFilePath : csvFilePaths) {
            auto fullPath = appendKuzuRootPath(csvFilePath);
            line.replace(line.find(csvFilePath), csvFilePath.length(), fullPath);
        }
        std::cout << "Starting to execute query: " << line << std::endl;
        auto result = conn.query(line);
        std::cout << "Executed query: " << line << std::endl;
        if (!result->isSuccess()) {
            throw Exception(stringFormat(
                "Failed to execute statement: {}.\nError: {}", line, result->getErrorMessage()));
        }
    }
}
void BaseGraphTest::createDB() {
    if (database != nullptr) {
        database.reset();
    }
    database = std::make_unique<main::Database>(databasePath, *systemConfig);
    spdlog::set_level(spdlog::level::info);
}

void BaseGraphTest::createConns(const std::set<std::string>& connNames) {
    if (connNames.size() == 0) { // impart a default connName
        conn = std::make_unique<main::Connection>(database.get());
    } else {
        for (auto connName : connNames) {
            if (connMap[connName] != nullptr) {
                connMap[connName].reset();
            }
            connMap[connName] = std::make_unique<main::Connection>(database.get());
        }
    }
}

void BaseGraphTest::createDBAndConn() {
    if (database != nullptr) {
        database.reset();
    }
    database = std::make_unique<main::Database>(databasePath, *systemConfig);
    conn = std::make_unique<main::Connection>(database.get());
    spdlog::set_level(spdlog::level::info);
}

void BaseGraphTest::initGraph() {
    if (conn) { // normal conn
        TestHelper::executeScript(getInputDir() + TestHelper::SCHEMA_FILE_NAME, *conn);
        TestHelper::executeScript(getInputDir() + TestHelper::COPY_FILE_NAME, *conn);
    } else {
        // choose a conn from connMap
        TestHelper::executeScript(
            getInputDir() + TestHelper::SCHEMA_FILE_NAME, *(connMap.begin()->second));
        TestHelper::executeScript(
            getInputDir() + TestHelper::COPY_FILE_NAME, *(connMap.begin()->second));
    }
}

void BaseGraphTest::commitOrRollbackConnection(
    bool isCommit, TransactionTestType transactionTestType) const {
    if (transactionTestType == TransactionTestType::NORMAL_EXECUTION) {
        if (isCommit) {
            conn->query("COMMIT");
        } else {
            conn->query("ROLLBACK");
        }
        conn->query("BEGIN TRANSACTION");
    } else {
        if (isCommit) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
        } else {
            conn->query("ROLLBACK_SKIP_CHECKPOINT");
        }
    }
}

} // namespace testing
} // namespace kuzu
