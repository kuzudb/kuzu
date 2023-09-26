#include "common/copier_config/copier_config.h"

#include "common/exception/copy.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace common {

FileType FileTypeUtils::getFileTypeFromExtension(const std::string& extension) {
    FileType fileType = fileTypeMap[extension];
    if (fileType == FileType::UNKNOWN) {
        throw CopyException("Unsupported file type " + extension);
    }
    return fileType;
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
    }
}

} // namespace common
} // namespace kuzu
