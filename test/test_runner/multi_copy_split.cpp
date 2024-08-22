#include "test_runner/multi_copy_split.h"

#include <fstream>
#include <random>

#include "common/exception/test.h"
#include "common/random_engine.h"
#include "common/string_utils.h"
#include "spdlog/spdlog.h"
#include "test_helper/test_helper.h"

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

void SplitMultiCopyRandom::genSeedIfNecessary() {
    if (seed.size() == 2) {
        return;
    }
    auto seedGen = pcg_extras::seed_seq_from<std::random_device>();
    seed.resize(2);
    seedGen.generate(seed.begin(), seed.end());
}

std::vector<uint32_t> SplitMultiCopyRandom::getLineEnds(std::string path) const {
    RandomEngine random(seed[0], seed[1]);
    spdlog::info("RANDOM GENERATOR used seed: {} {}", seed[0], seed[1]);
    std::ifstream file(path);
    if (!file.is_open()) {
        throw TestException(stringFormat("Error opening file: {}, errno: {}.", path, errno));
    }
    // determine # lines in each split file
    std::string line;
    uint32_t linecnt = 0;
    while (getline(file, line)) {
        if (!line.empty()) {
            linecnt++;
        }
    }
    std::vector<uint32_t> lineEnds;
    for (auto i = 0u; i < numSplit - 1; i++) {
        lineEnds.push_back(random.nextRandomInteger(linecnt));
    }
    lineEnds.push_back(linecnt);
    std::sort(lineEnds.begin(), lineEnds.end());
    return lineEnds;
}

void SplitMultiCopyRandom::init() {
    genSeedIfNecessary();

    const std::string tmpDir = TestHelper::getTempDir("multi_copy");
    auto totalFilePath = TestHelper::joinPath(tmpDir, tableName + ".csv");
    std::string loadQuery = "COPY (LOAD FROM {} RETURN *) TO '{}';";
    loadQuery = stringFormat(loadQuery, source, totalFilePath);
    spdlog::info("QUERY: {}", loadQuery);
    validateQuery(connection, loadQuery);

    auto lineEnds = getLineEnds(totalFilePath);

    auto lineIdx = 0u;
    std::vector<std::string> lengths;
    std::ifstream file(totalFilePath);
    for (auto& endLine : lineEnds) {
        lengths.push_back(std::to_string(endLine - lineIdx));
        auto currFilePath =
            TestHelper::joinPath(tmpDir, tableName + std::to_string(lengths.size()) + ".csv");
        std::ofstream outfile(currFilePath);
        if (!outfile.is_open()) {
            throw TestException(
                stringFormat("Error opening file: {}, errno: {}.", currFilePath, errno));
        }
        splitFilePaths.push_back(currFilePath);
        std::string line;
        for (; lineIdx < endLine; lineIdx++) {
            if (!getline(file, line)) {
                throw TestException(
                    stringFormat("Expected line number mismatch: {} to {}.", lineIdx, endLine));
            }
            outfile << line << "\n";
        }
    }
    spdlog::info("RANDOM MULTI COPY: split {} lines into {} chunks of {} lines",
        lineEnds[lineEnds.size() - 1], numSplit, StringUtils::join(lengths, ", "));
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
