#include "graph_test/base_graph_test.h"

#include <fstream>

#include "common/assert.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "spdlog/spdlog.h"
#include "test_helper/test_helper.h"
#include <common/string_utils.h>

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace testing {

void TestHelper::executeScript(const std::string& cypherScript, Connection& conn) {
    std::cout << "cypherScript: " << cypherScript << std::endl;
    if (!std::filesystem::exists(cypherScript)) {
        std::cout << "cypherScript: " << cypherScript << " doesn't exist. Skipping..." << std::endl;
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
                substrLower.find(".ttl") != std::string::npos ||
                substrLower.find(".nq") != std::string::npos ||
                substrLower.find(".json") != std::string::npos ||
                substrLower.find(".kuzu_extension") != std::string::npos) {
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
            throw Exception(stringFormat("Failed to execute statement: {}.\nError: {}", line,
                result->getErrorMessage()));
        }
    }
}

void BaseGraphTest::createConns(const std::set<std::string>& connNames) {
    if (connNames.size() == 0) { // impart a default connName
        conn = std::make_unique<Connection>(database.get());
        TestHelper::updateClientConfigFromEnv(*conn->getClientContext()->getClientConfigUnsafe());
    } else {
        for (auto connName : connNames) {
            if (connMap.contains(connName)) {
                throw RuntimeException(stringFormat(
                    "Cannot create connection with name {} because it already exists.", connName));
            }
            auto newConn = std::make_unique<Connection>(database.get());
            TestHelper::updateClientConfigFromEnv(
                *newConn->getClientContext()->getClientConfigUnsafe());
            connMap[connName] = std::move(newConn);
        }
    }
}

void BaseGraphTest::createDBAndConn() {
    if (database != nullptr) {
        database.reset();
    }
    database = std::make_unique<Database>(databasePath, *systemConfig);
    conn = std::make_unique<Connection>(database.get());
    spdlog::set_level(spdlog::level::info);
}

void BaseGraphTest::initGraph(const std::string& datasetDir) const {
    Connection* connection = conn ? conn.get() : (connMap.begin()->second).get();
    KU_ASSERT_UNCONDITIONAL(connection != nullptr);

    if (TestHelper::E2E_OVERRIDE_IMPORT_DIR.empty()) {
        TestHelper::executeScript(datasetDir + TestHelper::SCHEMA_FILE_NAME, *connection);
        TestHelper::executeScript(datasetDir + TestHelper::COPY_FILE_NAME, *connection);
        return;
    }

    // Run tests on datasets exported from a previous Kuzu version. Used to verify that exports and
    // imports across versions work correctly.Note: we skip importing the empty db
    auto dirs = StringUtils::split(StringUtils::getLower(datasetDir), "/");
    if (std::find(dirs.begin(), dirs.end(), "empty") != dirs.end()) {
        std::cout << "Skipping empty DB" << std::endl;
        return;
    }
    std::string query = "IMPORT DATABASE '" + datasetDir + "';";
    std::cout << "Loading database as: " << query << std::endl;
    auto result = connection->query(query);
    std::cout << "Executed query: " << query << std::endl;
    if (!result->isSuccess()) {
        throw Exception(stringFormat("Failed to execute statement: {}.\nError: {}", query,
            result->getErrorMessage()));
    }
}

} // namespace testing
} // namespace kuzu
