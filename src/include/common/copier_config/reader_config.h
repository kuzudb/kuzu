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

struct FileTypeUtils {
    static FileType getFileTypeFromExtension(std::string_view extension);
    static std::string toString(FileType fileType);
    static FileType fromString(std::string fileType);
};

struct ReaderConfig {
    FileType fileType = FileType::UNKNOWN;
    std::vector<std::string> filePaths;
    std::unordered_map<std::string, Value> options;

    ReaderConfig() = default;
    ReaderConfig(FileType fileType, std::vector<std::string> filePaths)
        : fileType{fileType}, filePaths{std::move(filePaths)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ReaderConfig);

    inline uint32_t getNumFiles() const { return filePaths.size(); }

private:
    ReaderConfig(const ReaderConfig& other)
        : fileType{other.fileType}, filePaths{other.filePaths}, options{other.options} {}
};

} // namespace common
} // namespace kuzu
