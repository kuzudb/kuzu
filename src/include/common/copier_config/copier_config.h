#pragma once

#include <fstream>

#include "common/constants.h"
#include "common/types/types_include.h"
#include "common/types/value.h"

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
    enum class FileType : uint8_t { UNKNOWN = 0, CSV = 1, PARQUET = 2, NPY = 3, TURTLE = 4 };
    FileType fileType;
    std::vector<std::string> filePaths;
    std::vector<std::string> columnNames;
    std::unique_ptr<CSVReaderConfig> csvReaderConfig;

    // Copy From
    CopyDescription(FileType fileType, const std::vector<std::string>& filePaths,
        std::unique_ptr<CSVReaderConfig> csvReaderConfig)
        : fileType{fileType}, filePaths{filePaths}, csvReaderConfig{std::move(csvReaderConfig)} {}

    // Copy To
    CopyDescription(FileType fileType, const std::vector<std::string>& filePaths,
        const std::vector<std::string>& columnNames)
        : fileType{fileType}, filePaths{filePaths}, columnNames{columnNames}, csvReaderConfig{
                                                                                  nullptr} {}

    CopyDescription(const CopyDescription& other)
        : fileType{other.fileType}, filePaths{other.filePaths}, columnNames{other.columnNames} {
        if (other.csvReaderConfig != nullptr) {
            this->csvReaderConfig = std::make_unique<CSVReaderConfig>(*other.csvReaderConfig);
        }
    }

    inline bool parallelRead() const {
        return fileType != FileType::CSV && fileType != FileType::TURTLE;
    }

    inline std::unique_ptr<CopyDescription> copy() const {
        assert(this);
        return std::make_unique<CopyDescription>(*this);
    }

    inline static std::unordered_map<std::string, FileType> fileTypeMap{
        {"unknown", FileType::UNKNOWN}, {".csv", FileType::CSV}, {".parquet", FileType::PARQUET},
        {".npy", FileType::NPY}, {".ttl", FileType::TURTLE}};

    static FileType getFileTypeFromExtension(const std::string& extension);
};

} // namespace common
} // namespace kuzu
