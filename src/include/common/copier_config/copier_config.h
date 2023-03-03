#pragma once

#include <fstream>

#include "common/constants.h"
#include "common/types/types_include.h"
#include "common/types/value.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace common {

struct CSVReaderConfig {
    CSVReaderConfig()
        : escapeChar{CopyConstants::DEFAULT_CSV_ESCAPE_CHAR},
          delimiter{CopyConstants::DEFAULT_CSV_DELIMITER},
          quoteChar{CopyConstants::DEFAULT_CSV_QUOTE_CHAR},
          listBeginChar{CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR},
          listEndChar{CopyConstants::DEFAULT_CSV_LIST_END_CHAR},
          hasHeader{CopyConstants::DEFAULT_CSV_HAS_HEADER} {}

    char escapeChar;
    char delimiter;
    char quoteChar;
    char listBeginChar;
    char listEndChar;
    bool hasHeader;
};

struct CopyDescription {
    CopyDescription(const std::string& filePath, CSVReaderConfig csvReaderConfig);

    CopyDescription(const CopyDescription& copyDescription);

    enum class FileType { CSV, ARROW, PARQUET };

    static std::string getFileTypeName(FileType fileType);

    static std::string getFileTypeSuffix(FileType fileType);

    void setFileType(std::string const& fileName);

    const std::string filePath;
    std::unique_ptr<CSVReaderConfig> csvReaderConfig;
    FileType fileType;
};

} // namespace common
} // namespace kuzu
