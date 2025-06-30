#include "graph_test/base_graph_test.h"

#include <fstream>

#include "common/assert.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "spdlog/spdlog.h"
#include "test_helper/test_helper.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace testing {

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
    // imports across versions work correctly. This skips importing the `empty` dataset.
    auto dirs = StringUtils::split(StringUtils::getLower(datasetDir), "/");
    if (std::find(dirs.begin(), dirs.end(), "empty") != dirs.end()) {
        std::cout << stringFormat("Skipping Empty Dataset {}", datasetDir) << std::endl;
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
