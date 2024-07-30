#include "test_runner/multi_copy_split.h"

#include <fstream>

#include "common/exception/test.h"
#include "common/file_system/local_file_system.h"
#include "common/random_engine.h"
#include "common/string_utils.h"
#include "spdlog/spdlog.h"
#include "test_helper/test_helper.h"
#include "test_runner/csv_to_parquet_converter.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

static std::unique_ptr<main::QueryResult> validateQuery(main::Connection& conn,
    std::string& query) {
    auto result = conn.query(query);
    if (!result->isSuccess()) {
        throw Exception(stringFormat("Failed to execute statement: {}.\nError: {}", query,
            result->getErrorMessage()));
    }
    return result;
}

void SplitMultiCopyRandom::init() {
    RandomEngine random;
    std::ifstream file(filePath);
    if (!file.is_open()) {
        throw TestException(stringFormat("Error opening file: {}, errno: {}.", filePath, errno));
    }
    std::string line;
    std::vector<std::string> contents;
    while (getline(file, line)) {
        if (!line.empty()) {
            contents.push_back(line);
        }
    }
    std::vector<uint32_t> endLines;
    for (auto i = 0u; i < numSplit - 1; i++) {
        endLines.push_back(random.nextRandomInteger(contents.size()));
    }
    endLines.push_back(contents.size());
    std::sort(endLines.begin(), endLines.end());
    auto lineIdx = 0u;
    std::vector<std::string> lengths;
    const std::string tmpDir = TestHelper::getTempDir("multi_copy");
    for (auto& endLine : endLines) {
        lengths.push_back(std::to_string(endLine - lineIdx));
        auto currFilePath =
            LocalFileSystem::joinPath(tmpDir, tableName + std::to_string(lineIdx) + ".csv");
        std::ofstream outfile(currFilePath);
        if (!outfile.is_open()) {
            throw TestException(
                stringFormat("Error opening file: {}, errno: {}.", currFilePath, errno));
        }
        splitFilePaths.push_back(currFilePath);
        for (; lineIdx < endLine; lineIdx++) {
            outfile << contents[lineIdx] << "\n";
        }
    }
    spdlog::info("RANDOM MULTI COPY: split {} into {} chunks of {} lines", 
        filePath, numSplit,  StringUtils::join(lengths, ", "));
}

void SplitMultiCopyRandom::run() {
    const std::string query = "COPY {} FROM \"{}\";";
    for (auto file : splitFilePaths) {
        auto currQuery = stringFormat(query, tableName, file);
        spdlog::info("QUERY: {}", currQuery);
        validateQuery(connection, currQuery);
    }
}

} // namespace testing
} // namespace kuzu
