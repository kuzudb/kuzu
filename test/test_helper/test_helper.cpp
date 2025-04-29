#include "test_helper/test_helper.h"

#include <chrono>
#include <fstream>

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

std::string TestHelper::getTempSuffix() {
    uint64_t val = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    return std::to_string(val);
}

} // namespace testing
} // namespace kuzu
