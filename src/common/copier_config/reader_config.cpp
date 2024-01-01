#include "common/copier_config/reader_config.h"

#include "common/assert.h"
#include "common/exception/copy.h"

namespace kuzu {
namespace common {

FileType FileTypeUtils::getFileTypeFromExtension(std::string_view extension) {
    if (extension == ".csv") {
        return FileType::CSV;
    }
    if (extension == ".parquet") {
        return FileType::PARQUET;
    }
    if (extension == ".npy") {
        return FileType::NPY;
    }
    if (extension == ".ttl") {
        return FileType::TURTLE;
    }
    if (extension == ".nq") {
        return FileType::NQUADS;
    }
    throw CopyException(std::string("Unsupported file type ").append(extension));
}

std::string FileTypeUtils::toString(FileType fileType) {
    switch (fileType) {
    case FileType::UNKNOWN: {
        return "UNKNOWN";
    }
    case FileType::CSV: {
        return "CSV";
    }
    case FileType::PARQUET: {
        return "PARQUET";
    }
    case FileType::NPY: {
        return "NPY";
    }
    case FileType::TURTLE: {
        return "TURTLE";
    }
    case FileType::NQUADS: {
        return "NQUADS";
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace common
} // namespace kuzu
