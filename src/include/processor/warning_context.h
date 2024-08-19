#pragma once

#include <vector>

#include "common/types/types.h"
#include "processor/operator/persistent/reader/csv/csv_error.h"

namespace kuzu {

namespace main {
class ClientContext;
}

namespace processor {

struct WarningSchema {
    static constexpr uint64_t getNumColumns() { return 5; }
    static std::vector<std::string> getColumnNames();
    static std::vector<common::LogicalType> getColumnDataTypes();
};

struct WarningInfo {
    uint64_t queryID;
    processor::PopulatedCSVError warning;

    explicit WarningInfo(processor::PopulatedCSVError warning, uint64_t queryID)
        : queryID(queryID), warning(std::move(warning)) {}
};

struct WarningContext {
    std::vector<WarningInfo> warnings;
    uint64_t warningLimit;
    std::mutex mtx;

    void appendWarningMessages(const std::vector<PopulatedCSVError>& messages,
        uint64_t messageLimit, uint64_t queryID);
};

} // namespace processor
} // namespace kuzu
