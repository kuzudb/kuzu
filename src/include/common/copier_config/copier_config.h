#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/constants.h"
#include "rdf_config.h"

namespace kuzu {
namespace common {

struct CSVOption {
    CSVOption()
        : escapeChar{CopyConstants::DEFAULT_CSV_ESCAPE_CHAR},
          delimiter{CopyConstants::DEFAULT_CSV_DELIMITER},
          quoteChar{CopyConstants::DEFAULT_CSV_QUOTE_CHAR},
          hasHeader{CopyConstants::DEFAULT_CSV_HAS_HEADER} {}

    virtual ~CSVOption() = default;

    virtual std::unique_ptr<CSVOption> copyCSVOption() const {
        return std::make_unique<CSVOption>(*this);
    }

    // TODO(Xiyang): Add newline character option and delimiter can be a string.
    char escapeChar;
    char delimiter;
    char quoteChar;
    bool hasHeader;
};

struct CSVReaderConfig : public CSVOption {
    bool parallel;

    CSVReaderConfig() : CSVOption{}, parallel{CopyConstants::DEFAULT_CSV_PARALLEL} {}

    inline std::unique_ptr<CSVReaderConfig> copy() {
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
    std::unique_ptr<CSVReaderConfig> csvReaderConfig = nullptr;
    // NOTE: Do not try to refactor this with CSVReaderConfig. We might remove this.
    std::unique_ptr<RdfReaderConfig> rdfReaderConfig;

    ReaderConfig(FileType fileType, std::vector<std::string> filePaths,
        std::unique_ptr<CSVReaderConfig> csvReaderConfig)
        : fileType{fileType}, filePaths{std::move(filePaths)}, csvReaderConfig{
                                                                   std::move(csvReaderConfig)} {}

    ReaderConfig(const ReaderConfig& other) : fileType{other.fileType}, filePaths{other.filePaths} {
        if (other.csvReaderConfig != nullptr) {
            this->csvReaderConfig = other.csvReaderConfig->copy();
        }
        if (other.rdfReaderConfig != nullptr) {
            this->rdfReaderConfig = other.rdfReaderConfig->copy();
        }
    }

    inline uint32_t getNumFiles() const { return filePaths.size(); }

    inline std::unique_ptr<ReaderConfig> copy() const {
        return std::make_unique<ReaderConfig>(*this);
    }
};

} // namespace common
} // namespace kuzu
