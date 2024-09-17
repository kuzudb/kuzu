#include "processor/warning_context.h"

#include <numeric>

#include "common/assert.h"
#include "common/uniq_lock.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {
WarningSourceData::WarningSourceData(uint64_t startByteOffset, uint64_t endByteOffset,
    common::idx_t fileIdx, uint64_t blockIdx, uint64_t rowOffsetInBlock)
    : startByteOffset(startByteOffset), endByteOffset(endByteOffset), fileIdx(fileIdx),
      blockIdx(blockIdx), rowOffsetInBlock(rowOffsetInBlock) {}

CSVError::CSVError(std::string message, WarningSourceData warningData, bool completedLine,
    bool mustThrow)
    : message(std::move(message)), completedLine(completedLine),
      warningData(std::move(warningData)), mustThrow(mustThrow) {}

bool CSVError::operator<(const CSVError& o) const {
    if (warningData.blockIdx == o.warningData.blockIdx) {
        return warningData.rowOffsetInBlock < o.warningData.rowOffsetInBlock;
    }
    return warningData.blockIdx < o.warningData.blockIdx;
}

WarningContext::WarningContext(main::ClientConfig* clientConfig)
    : clientConfig(clientConfig), queryWarningCount(0), numStoredWarnings(0) {}

void WarningContext::appendWarningMessages(const std::vector<CSVError>& messages) {
    common::UniqLock lock{mtx};

    queryWarningCount += messages.size();

    for (const auto& message : messages) {
        if (numStoredWarnings >= clientConfig->warningLimit) {
            break;
        }
        unpopulatedWarnings[message.warningData.fileIdx].warnings.push_back(message);
        ++numStoredWarnings;
    }
}

const std::vector<WarningInfo>& WarningContext::getPopulatedWarnings() const {
    // if there are still unpopulated warnings when we try to get populated warnings something is
    // probably wrong
    KU_ASSERT(0 == std::accumulate(unpopulatedWarnings.begin(), unpopulatedWarnings.end(), 0,
                       [](auto a, auto b) { return a + b.second.warnings.size(); }));
    return populatedWarnings;
}

void WarningContext::defaultPopulateAllWarnings(uint64_t queryID) {
    for (const auto& [fileIdx, _] : unpopulatedWarnings) {
        populateWarnings(fileIdx, queryID);
    }
}

void WarningContext::populateWarnings(common::idx_t fileIdx, uint64_t queryID,
    std::optional<populate_func_t> populateFunc, SerialCSVReader* populateReader) {
    if (!populateFunc.has_value()) {
        // if no populate functor is provided we default to just copying the message over
        // and leaving the CSV fields unpopulated
        populateFunc = [](CSVError error, SerialCSVReader*) -> PopulatedCSVError {
            return PopulatedCSVError{.message = std::move(error.message),
                .filePath = "",
                .skippedLine = "",
                .lineNumber = 0};
        };
    }
    for (auto& warning : unpopulatedWarnings[fileIdx].warnings) {
        populatedWarnings.emplace_back(populateFunc.value()(std::move(warning), populateReader),
            queryID);
    }
    unpopulatedWarnings[fileIdx].warnings.clear();
}

void WarningContext::clearPopulatedWarnings() {
    populatedWarnings.clear();
    numStoredWarnings = 0;
}

uint64_t WarningContext::getWarningCount(uint64_t) {
    auto ret = queryWarningCount;
    queryWarningCount = 0;
    return ret;
}

void WarningContext::setIgnoreErrorsForCurrentQuery(bool ignoreErrors) {
    ignoreErrorsOption = ignoreErrors;
}

bool WarningContext::getIgnoreErrorsOption() const {
    return ignoreErrorsOption;
}

} // namespace processor
} // namespace kuzu
