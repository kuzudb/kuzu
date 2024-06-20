#include "common/copier_config/reader_config.h"

#include "common/assert.h"
#include "common/exception/binder.h"
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
    if (extension == ".nt") {
        return FileType::NTRIPLES;
    }
    if (extension == ".json") {
        return FileType::JSON;
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
    case FileType::NTRIPLES: {
        return "NTRIPLES";
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

FileType FileTypeUtils::fromString(std::string fileType) {
    if (fileType == "CSV") {
        return FileType::CSV;
    } else if (fileType == "PARQUET") {
        return FileType::PARQUET;
    } else if (fileType == "NPY") {
        return FileType::NPY;
    } else {
        throw BinderException(stringFormat("Unsupported file type: {}.", fileType));
    }
}

} // namespace common
} // namespace kuzu
