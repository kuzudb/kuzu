#pragma once

#include <mutex>
#include <string>
#include <vector>

#include "main/client_config.h"

namespace kuzu {

namespace processor {

struct PopulatedCSVError {
    std::string message;
    std::string filePath;
    std::string skippedLine;
    uint64_t lineNumber;
};

struct WarningInfo {
    uint64_t queryID;
    processor::PopulatedCSVError warning;

    WarningInfo(processor::PopulatedCSVError warning, uint64_t queryID)
        : queryID(queryID), warning(std::move(warning)) {}
};

struct WarningContext {
    std::vector<WarningInfo> warnings;
    main::ClientConfig* clientConfig;
    std::mutex mtx;

    explicit WarningContext(main::ClientConfig* clientConfig);

    void appendWarningMessages(const std::vector<PopulatedCSVError>& messages, uint64_t queryID);
};

} // namespace processor
} // namespace kuzu
