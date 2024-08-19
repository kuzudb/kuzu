#pragma once

#include <mutex>
#include <string>
#include <vector>

namespace kuzu {

namespace processor {

struct PopulatedCSVError {
    std::string message;
    std::string filePath;
    std::string reconstructedLine;
    uint64_t lineNumber;
};

struct WarningInfo {
    uint64_t queryID;
    processor::PopulatedCSVError warning;

    explicit WarningInfo(processor::PopulatedCSVError warning, uint64_t queryID)
        : queryID(queryID), warning(std::move(warning)) {}
};

struct WarningContext {
    std::vector<WarningInfo> warnings;
    uint64_t* warningLimit;
    std::mutex mtx;

    explicit WarningContext(uint64_t* warningLimit) : warningLimit(warningLimit) {}

    void appendWarningMessages(const std::vector<PopulatedCSVError>& messages, uint64_t queryID);
};

} // namespace processor
} // namespace kuzu
