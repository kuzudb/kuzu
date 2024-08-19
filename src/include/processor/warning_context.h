#pragma once

#include <vector>

#include "common/types/types.h"
#include "processor/operator/persistent/reader/csv/csv_error.h"

namespace kuzu {

namespace main {
class ClientContext;
}

namespace processor {

struct WarningInfo {
    uint64_t queryID;
    processor::PopulatedCSVError warning;

    explicit WarningInfo(processor::PopulatedCSVError warning, uint64_t queryID)
        : queryID(queryID), warning(std::move(warning)) {}
};

struct WarningContext {
    std::vector<WarningInfo> warnings;
    std::mutex mtx;

    void appendWarningMessages(const std::vector<PopulatedCSVError>& messages, uint64_t queryID);
};

} // namespace processor
} // namespace kuzu
