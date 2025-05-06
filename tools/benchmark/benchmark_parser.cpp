#include "benchmark_parser.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>

#include "common/assert.h"
#include "common/exception/exception.h"
#include "common/file_system/file_system.h"
#include "common/string_utils.h"
#include "common/system_config.h"

namespace kuzu {
namespace benchmark {

std::vector<std::unique_ptr<ParsedBenchmark>> BenchmarkParser::parseBenchmarkFile(
    const std::string& path, bool checkOutputOrder) {
    std::vector<std::unique_ptr<ParsedBenchmark>> result;
    if (access(path.c_str(), 0) != 0) {
        throw common::Exception("Test file not exists! [" + path + "].");
    }
    struct stat status {};
    stat(path.c_str(), &status);
    if (status.st_mode & S_IFDIR) {
        throw common::Exception("Test file is a directory. [" + path + "].");
    }
    std::ifstream ifs(path);
    std::string line;
    ParsedBenchmark* currentConfig = nullptr;
    while (getline(ifs, line)) {
        if (line.starts_with("-NAME")) {
            auto config = std::make_unique<ParsedBenchmark>();
            currentConfig = config.get();
            result.push_back(std::move(config));
            currentConfig->name = line.substr(6, line.length());
        } else if (line.starts_with("-QUERY")) {
            KU_ASSERT(currentConfig);
            currentConfig->query = line.substr(7, line.length());
            replaceVariables(currentConfig->query);
        } else if (line.starts_with("-PRERUN")) {
            KU_ASSERT(currentConfig);
            currentConfig->preRun = line.substr(8, line.length());
            replaceVariables(currentConfig->preRun);
        } else if (line.starts_with("-POSTRUN")) {
            KU_ASSERT(currentConfig);
            replaceVariables(currentConfig->postRun);
            currentConfig->postRun = line.substr(9, line.length());
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

void BenchmarkParser::replaceVariables(std::string& str) const {
    for (auto& variable : variableMap) {
        common::StringUtils::replaceAll(str, "${" + variable.first + "}", variable.second);
    }
}

} // namespace benchmark
} // namespace kuzu
