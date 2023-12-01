#pragma once

#include <memory>
#include <string>
#include <vector>

namespace kuzu {
namespace common {

enum class FileType : uint8_t {
    UNKNOWN = 0,
    CSV = 1,
    PARQUET = 2,
    NPY = 3,
    TURTLE = 4, // Terse triples http://www.w3.org/TR/turtle
    NQUADS = 5  // Line-based quads http://www.w3.org/TR/n-quads/
};

struct FileTypeUtils {
    static FileType getFileTypeFromExtension(std::string_view extension);
    static std::string toString(FileType fileType);
};

struct ExtraReaderConfig {
    virtual ~ExtraReaderConfig() = default;
    virtual std::unique_ptr<ExtraReaderConfig> copy() = 0;
};

struct ReaderConfig {
    FileType fileType = FileType::UNKNOWN;
    std::vector<std::string> filePaths;
    std::unique_ptr<ExtraReaderConfig> extraConfig;

    ReaderConfig(FileType fileType, std::vector<std::string> filePaths,
        std::unique_ptr<ExtraReaderConfig> extraConfig)
        : fileType{fileType}, filePaths{std::move(filePaths)}, extraConfig{std::move(extraConfig)} {
    }
    ReaderConfig(const ReaderConfig& other) : fileType{other.fileType}, filePaths{other.filePaths} {
        if (other.extraConfig != nullptr) {
            extraConfig = other.extraConfig->copy();
        }
    }
    ReaderConfig(ReaderConfig&& other) = default;

    inline uint32_t getNumFiles() const { return filePaths.size(); }

    inline std::unique_ptr<ReaderConfig> copy() const {
        return std::make_unique<ReaderConfig>(*this);
    }
};

} // namespace common
} // namespace kuzu
