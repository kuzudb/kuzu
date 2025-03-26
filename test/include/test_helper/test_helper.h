#pragma once

#include <filesystem>

#include "common/file_system/file_system.h"
#include "common/string_utils.h"
#include "common/system_config.h"
#include "main/kuzu.h"

namespace kuzu {
namespace testing {

struct TestQueryConfig {
    std::string name;
    std::string query;
    uint64_t numThreads = 4;
    uint64_t expectedNumTuples = 0;
    std::vector<std::string> expectedTuples;
    bool checkOutputOrder = false;
    bool compareResult = true;
};

class TestHelper {
public:
    inline static std::string E2E_TEST_FILES_DIRECTORY = "test/test_files";
    static constexpr char SCHEMA_FILE_NAME[] = "schema.cypher";
    static constexpr char COPY_FILE_NAME[] = "copy.cypher";
    static constexpr char TEST_ANSWERS_PATH[] = "test/answers";
    static constexpr char TEST_STATEMENTS_PATH[] = "test/statements";
    static constexpr char DEFAULT_CONN_NAME[] = "conn_default";

    // At the moment the 6 constant is mostly just to get tests to pass when using the larger page
    // size configs (including tests which have high peak hash index memory usage) It should be
    // tweaked to a more accurate value when the hash index is capable of reducing its memory usage
    static constexpr uint64_t HASH_INDEX_MEM =
        common::HashIndexConstants::NUM_HASH_INDEXES * common::KUZU_PAGE_SIZE * 8;
    static constexpr uint64_t DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING =
        (1ull << 26) /* (64MB) */ + HASH_INDEX_MEM;

    static void setE2ETestFilesDirectory(const std::string& directory) {
        E2E_TEST_FILES_DIRECTORY = directory;
    }

    static std::vector<std::unique_ptr<TestQueryConfig>> parseTestFile(const std::string& path,
        bool checkOutputOrder = false);

    static std::vector<std::string> convertResultToString(main::QueryResult& queryResult,
        bool checkOutputOrder = false);

    static void executeScript(const std::string& cypherScript, main::Connection& conn);

    static std::string getTestListFile() {
        return appendKuzuRootPath(std::string(E2E_TEST_FILES_DIRECTORY) + "/test_list");
    }

    static std::string appendKuzuRootPath(const std::string& path) {
        if (std::filesystem::path(path).is_relative()) {
            return KUZU_ROOT_DIRECTORY + std::string("/") + path;
        } else {
            return path;
        }
    }

    static std::unique_ptr<main::SystemConfig> getSystemConfigFromEnv() {
        auto systemConfig = std::make_unique<main::SystemConfig>();
        auto bufferPoolSizeEnv = getSystemEnv("BUFFER_POOL_SIZE");
        auto maxNumThreadsEnv = getSystemEnv("MAX_NUM_THREADS");
        auto enableCompressionEnv = getSystemEnv("ENABLE_COMPRESSION");
        auto checkpointThresholdEnv = getSystemEnv("CHECKPOINT_THRESHOLD");
        systemConfig->bufferPoolSize = bufferPoolSizeEnv.empty() ?
                                           DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING :
                                           std::stoull(bufferPoolSizeEnv);
        systemConfig->maxNumThreads = maxNumThreadsEnv.empty() ? 2 : std::stoull(maxNumThreadsEnv);
        if (!enableCompressionEnv.empty()) {
            systemConfig->enableCompression = enableCompressionEnv == "true";
        }
        systemConfig->checkpointThreshold = checkpointThresholdEnv.empty() ?
                                                systemConfig->checkpointThreshold :
                                                std::stoull(checkpointThresholdEnv);
        return systemConfig;
    }

    static std::string getTempSuffix();

    static std::filesystem::path getTempDir() {
        auto tempDir = std::getenv("RUNNER_TEMP");
        if (tempDir != nullptr) {
            return std::filesystem::path(tempDir) / "kuzu";
        } else {
            return std::filesystem::temp_directory_path() / "kuzu";
        }
    }

    static std::string getTempDir(const std::string& name) {
        const auto path = getTempDir() / (name + getTempSuffix());
        std::filesystem::create_directories(path);
        auto pathStr = path.string();
#ifdef _WIN32
        // kuzu still doesn't support backslashes in paths on windows
        std::replace(pathStr.begin(), pathStr.end(), '\\', '/');
#endif
        return pathStr;
    }

    static bool isSystemEnvValid(const char* env) { return env != nullptr && strlen(env) > 0; }
    static std::string getSystemEnv(const char* key) {
        const auto env = std::getenv(key);
        if (isSystemEnvValid(env)) {
            auto envStr = std::string(env);
            common::StringUtils::toLower(envStr);
            return envStr;
        }
        return "";
    }

    inline static std::string joinPath(const std::string& base, const std::string& part) {
        auto pathStr = common::FileSystem::joinPath(base, part);
#ifdef _WIN32
        // kuzu still doesn't support backslashes in paths on windows
        std::replace(pathStr.begin(), pathStr.end(), '\\', '/');
#endif
        return pathStr;
    }

private:
    static void initializeConnection(TestQueryConfig* config, main::Connection& conn);
};

} // namespace testing
} // namespace kuzu
