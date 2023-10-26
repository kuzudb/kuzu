#pragma once

#include <cassert>
#include <fstream>
#include <memory>
#include <vector>

#include "common/constants.h"
#include "common/table_type.h"
#include "common/types/types.h"
#include "rdf_config.h"

namespace kuzu {
namespace common {

struct CSVReaderConfig {
    char escapeChar;
    char delimiter;
    char quoteChar;
    char listBeginChar;
    char listEndChar;
    bool hasHeader;
    bool parallel;

    CSVReaderConfig()
        : escapeChar{CopyConstants::DEFAULT_CSV_ESCAPE_CHAR},
          delimiter{CopyConstants::DEFAULT_CSV_DELIMITER},
          quoteChar{CopyConstants::DEFAULT_CSV_QUOTE_CHAR},
          listBeginChar{CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR},
          listEndChar{CopyConstants::DEFAULT_CSV_LIST_END_CHAR},
          hasHeader{CopyConstants::DEFAULT_CSV_HAS_HEADER},
          parallel{CopyConstants::DEFAULT_CSV_PARALLEL} {}

    CSVReaderConfig(const CSVReaderConfig& other)
        : escapeChar{other.escapeChar}, delimiter{other.delimiter}, quoteChar{other.quoteChar},
          listBeginChar{other.listBeginChar},
          listEndChar{other.listEndChar}, hasHeader{other.hasHeader}, parallel{other.parallel} {}

    inline std::unique_ptr<CSVReaderConfig> copy() const {
        return std::make_unique<CSVReaderConfig>(*this);
    }
};

enum class FileType : uint8_t { UNKNOWN = 0, CSV = 1, PARQUET = 2, NPY = 3, TURTLE = 4 };

struct FileTypeUtils {
    static FileType getFileTypeFromExtension(const std::string& extension);
    static std::string toString(FileType fileType);
};

struct ReaderConfig {
    FileType fileType = FileType::UNKNOWN;
    std::vector<std::string> filePaths;
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
    std::unique_ptr<CSVReaderConfig> csvReaderConfig = nullptr;
    // NOTE: Do not try to refactor this with CSVReaderConfig. We might remove this.
    std::unique_ptr<RdfReaderConfig> rdfReaderConfig;

    ReaderConfig(FileType fileType, std::vector<std::string> filePaths,
        std::unique_ptr<CSVReaderConfig> csvReaderConfig)
        : fileType{fileType}, filePaths{std::move(filePaths)}, csvReaderConfig{
                                                                   std::move(csvReaderConfig)} {}
    ReaderConfig(FileType fileType, std::vector<std::string> filePaths,
        std::vector<std::string> columnNames,
        std::vector<std::unique_ptr<common::LogicalType>> columnTypes)
        : fileType{fileType}, filePaths{std::move(filePaths)}, columnNames{std::move(columnNames)},
          columnTypes{std::move(columnTypes)} {}

    ReaderConfig(const ReaderConfig& other)
        : fileType{other.fileType}, filePaths{other.filePaths}, columnNames{other.columnNames},
          columnTypes{LogicalType::copy(other.columnTypes)} {
        if (other.csvReaderConfig != nullptr) {
            this->csvReaderConfig = other.csvReaderConfig->copy();
        }
        if (other.rdfReaderConfig != nullptr) {
            this->rdfReaderConfig = other.rdfReaderConfig->copy();
        }
    }

    inline bool csvParallelRead(TableType tableType) const {
        return tableType != TableType::REL && csvReaderConfig->parallel;
    }

    inline bool parallelRead(TableType tableType) const {
        return (fileType != FileType::CSV || csvParallelRead(tableType)) &&
               fileType != FileType::TURTLE;
    }
    inline uint32_t getNumFiles() const { return filePaths.size(); }
    inline uint32_t getNumColumns() const { return columnNames.size(); }

    inline std::unique_ptr<ReaderConfig> copy() const {
        return std::make_unique<ReaderConfig>(*this);
    }
};

} // namespace common
} // namespace kuzu
