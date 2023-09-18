#pragma once

#include <cassert>
#include <fstream>
#include <memory>
#include <vector>

#include "common/constants.h"

namespace kuzu {
namespace common {

struct CSVReaderConfig {
    char escapeChar;
    char delimiter;
    char quoteChar;
    char listBeginChar;
    char listEndChar;
    bool hasHeader;

    CSVReaderConfig()
        : escapeChar{CopyConstants::DEFAULT_CSV_ESCAPE_CHAR},
          delimiter{CopyConstants::DEFAULT_CSV_DELIMITER},
          quoteChar{CopyConstants::DEFAULT_CSV_QUOTE_CHAR},
          listBeginChar{CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR},
          listEndChar{CopyConstants::DEFAULT_CSV_LIST_END_CHAR},
          hasHeader{CopyConstants::DEFAULT_CSV_HAS_HEADER} {}
    CSVReaderConfig(const CSVReaderConfig& other)
        : escapeChar{other.escapeChar}, delimiter{other.delimiter}, quoteChar{other.quoteChar},
          listBeginChar{other.listBeginChar},
          listEndChar{other.listEndChar}, hasHeader{other.hasHeader} {}

    inline std::unique_ptr<CSVReaderConfig> copy() const {
        return std::make_unique<CSVReaderConfig>(*this);
    }
};

struct CopyDescription {
    enum class FileType : uint8_t { UNKNOWN = 0, CSV = 1, PARQUET = 2, NPY = 3, TURTLE = 4 };
    FileType fileType;
    std::vector<std::string> filePaths;
    std::vector<std::string> columnNames;
    std::unique_ptr<CSVReaderConfig> csvReaderConfig;

    CopyDescription(FileType fileType, const std::vector<std::string>& filePaths,
        std::unique_ptr<CSVReaderConfig> csvReaderConfig)
        : fileType{fileType}, filePaths{filePaths}, csvReaderConfig{std::move(csvReaderConfig)} {}

    CopyDescription(const CopyDescription& other)
        : fileType{other.fileType}, filePaths{other.filePaths}, columnNames{other.columnNames} {
        if (other.csvReaderConfig != nullptr) {
            this->csvReaderConfig = std::make_unique<CSVReaderConfig>(*other.csvReaderConfig);
        }
    }

    inline bool parallelRead() const {
        return fileType != FileType::CSV && fileType != FileType::TURTLE;
    }
    inline uint32_t getNumFiles() const { return filePaths.size(); }

    inline std::unique_ptr<CopyDescription> copy() const {
        return std::make_unique<CopyDescription>(*this);
    }

    inline static std::unordered_map<std::string, FileType> fileTypeMap{
        {"unknown", FileType::UNKNOWN}, {".csv", FileType::CSV}, {".parquet", FileType::PARQUET},
        {".npy", FileType::NPY}, {".ttl", FileType::TURTLE}};

    static FileType getFileTypeFromExtension(const std::string& extension);
};

} // namespace common
} // namespace kuzu
