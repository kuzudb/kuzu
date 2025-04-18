#include "test_helper/test_helper.h"

#include <chrono>
#include <fstream>

#include "spdlog/spdlog.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::main;

namespace kuzu {
namespace testing {

// Deprecated
std::vector<std::unique_ptr<TestQueryConfig>> TestHelper::parseTestFile(const std::string& path,
    bool checkOutputOrder) {
    std::vector<std::unique_ptr<TestQueryConfig>> result;
    if (access(path.c_str(), 0) != 0) {
        throw Exception("Test file not exists! [" + path + "].");
    }
    struct stat status {};
    stat(path.c_str(), &status);
    if (status.st_mode & S_IFDIR) {
        throw Exception("Test file is a directory. [" + path + "].");
    }
    std::ifstream ifs(path);
    std::string line;
    TestQueryConfig* currentConfig = nullptr;
    while (getline(ifs, line)) {
        if (line.starts_with("-NAME")) {
            auto config = std::make_unique<TestQueryConfig>();
            currentConfig = config.get();
            result.push_back(std::move(config));
            currentConfig->name = line.substr(6, line.length());
        } else if (line.starts_with("-QUERY")) {
            KU_ASSERT(currentConfig);
            currentConfig->query = line.substr(7, line.length());
        } else if (line.starts_with("-PARALLELISM")) {
            KU_ASSERT(currentConfig);
            currentConfig->numThreads = stoi(line.substr(13, line.length()));
        } else if (line.starts_with("-SKIP_COMPARE_RESULT")) {
            KU_ASSERT(currentConfig);
            currentConfig->compareResult = false;
        } else if (line.starts_with("----")) {
            uint64_t numTuples = stoi(line.substr(5, line.length()));
            KU_ASSERT(currentConfig);
            currentConfig->expectedNumTuples = numTuples;
            if (currentConfig->compareResult) {
                for (auto i = 0u; i < numTuples; i++) {
                    getline(ifs, line);
                    currentConfig->expectedTuples.push_back(line);
                }
                if (!checkOutputOrder) { // order is not important for result
                    sort(currentConfig->expectedTuples.begin(),
                        currentConfig->expectedTuples.end());
                }
            }
        }
    }
    return result;
}

std::vector<std::string> TestHelper::convertResultToString(QueryResult& queryResult,
    bool checkOutputOrder) {
    std::vector<std::string> actualOutput;
    while (queryResult.hasNext()) {
        auto tuple = queryResult.getNext();
        actualOutput.push_back(tuple->toString(std::vector<uint32_t>(tuple->len(), 0)));
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

std::string TestHelper::getTempSuffix() {
    uint64_t val = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    return std::to_string(val);
}

} // namespace testing
} // namespace kuzu
