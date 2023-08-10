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
    enum class FileType : uint8_t { UNKNOWN = 0, CSV = 1, PARQUET = 2, NPY = 3 };

    // Copy From
    CopyDescription(const std::vector<std::string>& filePaths, FileType fileType,
        CSVReaderConfig csvReaderConfig);

    // Copy To
    CopyDescription(const std::vector<std::string>& filePaths, FileType fileType,
        const std::vector<std::string>& columnNames);

    CopyDescription(const CopyDescription& copyDescription);

    inline static std::unordered_map<std::string, FileType> fileTypeMap{
        {"unknown", FileType::UNKNOWN}, {".csv", FileType::CSV}, {".parquet", FileType::PARQUET},
        {".npy", FileType::NPY}};

    static FileType getFileTypeFromExtension(const std::string& extension);

    static std::string getFileTypeName(FileType fileType);

    const std::vector<std::string> filePaths;
    const std::vector<std::string> columnNames;
    std::unique_ptr<CSVReaderConfig> csvReaderConfig;
    FileType fileType;
};

} // namespace common
} // namespace kuzu
