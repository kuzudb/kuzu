#pragma once

#include <string>
#include <vector>

#include "common/copy_constructors.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace common {

enum class FileType : uint8_t {
    UNKNOWN = 0,
    CSV = 1,
    PARQUET = 2,
    NPY = 3,
    TURTLE = 4,   // Terse triples http://www.w3.org/TR/turtle
    NQUADS = 5,   // Line-based quads http://www.w3.org/TR/n-quads/
    NTRIPLES = 6, // Line-based triples http://www.w3.org/TR/n-triples/
};

struct FileTypeInfo {
    FileType fileType;
    std::string fileTypeStr;
};

struct FileTypeUtils {
    static FileType getFileTypeFromExtension(std::string_view extension);
    static std::string toString(FileType fileType);
    static FileType fromString(std::string fileType);
};

struct ReaderConfig {
    FileTypeInfo fileTypeInfo;
    std::vector<std::string> filePaths;
    std::unordered_map<std::string, Value> options;

    ReaderConfig() : fileTypeInfo{FileType::UNKNOWN, ""} {}
    ReaderConfig(FileTypeInfo fileTypeInfo, std::vector<std::string> filePaths)
        : fileTypeInfo{std::move(fileTypeInfo)}, filePaths{std::move(filePaths)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ReaderConfig);

    uint32_t getNumFiles() const { return filePaths.size(); }
    std::string getFilePath(idx_t fileIdx) const {
        KU_ASSERT(fileIdx < getNumFiles());
        return filePaths[fileIdx];
    }

    template<typename T>
    T getOption(std::string optionName, T defaultValue) const {
        const auto optionIt = options.find(optionName);
        if (optionIt != options.end()) {
            return optionIt->second.getValue<T>();
        } else {
            return defaultValue;
        }
    }

private:
    ReaderConfig(const ReaderConfig& other)
        : fileTypeInfo{other.fileTypeInfo}, filePaths{other.filePaths}, options{other.options} {}
};

} // namespace common
} // namespace kuzu
