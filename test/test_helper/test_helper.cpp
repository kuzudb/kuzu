#include "test_helper/test_helper.h"

#include <chrono>
#include <fstream>
#include <iostream>

#include "spdlog/spdlog.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::main;

namespace kuzu {
namespace testing {

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
#ifdef __STATIC_LINK_EXTENSION_TEST__
        if (line.starts_with("load extension")) {
            continue;
        }
#endif
        std::cout << "Starting to execute query: " << line << std::endl;
        auto result = conn.query(line);
        std::cout << "Executed query: " << line << std::endl;
        if (!result->isSuccess()) {
            throw Exception(stringFormat("Failed to execute statement: {}.\nError: {}", line,
                result->getErrorMessage()));
        }
    }
}

std::unique_ptr<SystemConfig> TestHelper::getSystemConfigFromEnv() {
    auto systemConfig = std::make_unique<SystemConfig>();
    auto bufferPoolSizeEnv = getSystemEnv("BUFFER_POOL_SIZE");
    auto maxNumThreadsEnv = getSystemEnv("MAX_NUM_THREADS");
    auto enableCompressionEnv = getSystemEnv("ENABLE_COMPRESSION");
    auto checkpointThresholdEnv = getSystemEnv("CHECKPOINT_THRESHOLD");
    auto forceCheckpointOnCloseEnv = getSystemEnv("FORCE_CHECKPOINT_ON_CLOSE");
    auto maxDBSizeEnv = getSystemEnv("MAX_DB_SIZE");
    systemConfig->bufferPoolSize = bufferPoolSizeEnv.empty() ?
                                       DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING :
                                       std::stoull(bufferPoolSizeEnv);
    systemConfig->maxNumThreads = maxNumThreadsEnv.empty() ? 2 : std::stoull(maxNumThreadsEnv);
    if (!enableCompressionEnv.empty()) {
        systemConfig->enableCompression =
            StringUtils::caseInsensitiveEquals(enableCompressionEnv, "true");
    }
    systemConfig->checkpointThreshold = checkpointThresholdEnv.empty() ?
                                            systemConfig->checkpointThreshold :
                                            std::stoull(checkpointThresholdEnv);
    systemConfig->forceCheckpointOnClose =
        forceCheckpointOnCloseEnv.empty() ?
            systemConfig->forceCheckpointOnClose :
            StringUtils::caseInsensitiveEquals(forceCheckpointOnCloseEnv, "true");
    systemConfig->maxDBSize =
        maxDBSizeEnv.empty() ? systemConfig->maxDBSize : std::stoull(maxDBSizeEnv);
    return systemConfig;
}

std::string TestHelper::getTempSuffix() {
    uint64_t val = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    return std::to_string(val);
}

} // namespace testing
} // namespace kuzu
