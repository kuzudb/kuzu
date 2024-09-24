#pragma once

#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/api.h"
#include "common/types/types.h"
#include "main/client_config.h"

namespace kuzu {

namespace processor {

class SerialCSVReader;

// TODO(Royi) for performance reasons we may want to reduce the number of fields here since each
// field is essentially an extra column during copy
struct WarningSourceData {
    WarningSourceData(uint64_t startByteOffset, uint64_t endByteOffset, common::idx_t fileIdx,
        uint64_t blockIdx, uint64_t rowOffsetInBlock);

    WarningSourceData() = default;
    uint64_t startByteOffset;
    uint64_t endByteOffset;
    uint32_t fileIdx;
    uint64_t blockIdx;
    uint32_t rowOffsetInBlock;
};

struct LineContext {
    uint64_t startByteOffset;
    uint64_t endByteOffset;

    bool isCompleteLine;

    void setNewLine(uint64_t start);
    void setEndOfLine(uint64_t end);
};

// If parsing in parallel during parsing we may not be able to determine line numbers
// Thus we have additional fields that can be used to determine line numbers + reconstruct lines
// After parsing this will be used to populate a PopulatedCSVError instance
struct CSVError {
    CSVError(std::string message, WarningSourceData warningData, bool completedLine,
        bool mustThrow = false);

    std::string message;
    bool completedLine;
    WarningSourceData warningData;

    bool mustThrow;

    bool operator<(const CSVError& o) const;
};

struct PopulatedCSVError {
    std::string message;
    std::string filePath;
    std::string skippedLine;
    uint64_t lineNumber;
};

struct WarningInfo {
    uint64_t queryID;
    PopulatedCSVError warning;

    WarningInfo(PopulatedCSVError warning, uint64_t queryID)
        : queryID(queryID), warning(std::move(warning)) {}
};

struct UnpopulatedWarnings {
    std::vector<CSVError> warnings;
};

using populate_func_t = std::function<PopulatedCSVError(CSVError, SerialCSVReader*)>;

class KUZU_API WarningContext {
public:
    explicit WarningContext(main::ClientConfig* clientConfig);

    void appendWarningMessages(const std::vector<CSVError>& messages);

    void populateWarnings(common::idx_t fileIdx, uint64_t queryID,
        std::optional<populate_func_t> populateFunc = {},
        SerialCSVReader* populateReader = nullptr);
    void defaultPopulateAllWarnings(uint64_t queryID);

    const std::vector<WarningInfo>& getPopulatedWarnings() const;
    uint64_t getWarningCount(uint64_t queryID);
    void clearPopulatedWarnings();

    void setIgnoreErrorsForCurrentQuery(bool ignoreErrors);
    bool getIgnoreErrorsOption() const;

private:
    std::mutex mtx;
    main::ClientConfig* clientConfig;
    std::map<uint64_t, UnpopulatedWarnings> unpopulatedWarnings;
    std::vector<WarningInfo> populatedWarnings;
    uint64_t queryWarningCount;
    uint64_t numStoredWarnings;
    bool ignoreErrorsOption;
};

} // namespace processor
} // namespace kuzu
