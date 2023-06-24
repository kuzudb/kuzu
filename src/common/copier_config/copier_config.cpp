#include "common/copier_config/copier_config.h"

#include "common/type_utils.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace common {
CopyDescription::CopyDescription(
    const std::vector<std::string>& filePaths, CSVReaderConfig csvReaderConfig, FileType fileType)
    : filePaths{filePaths}, csvReaderConfig{nullptr}, fileType{fileType} {
    if (fileType == FileType::CSV) {
        this->csvReaderConfig = std::make_unique<CSVReaderConfig>(csvReaderConfig);
    }
}

CopyDescription::CopyDescription(const CopyDescription& copyDescription)
    : filePaths{copyDescription.filePaths},
      csvReaderConfig{nullptr}, fileType{copyDescription.fileType} {
    if (fileType == FileType::CSV) {
        this->csvReaderConfig = std::make_unique<CSVReaderConfig>(*copyDescription.csvReaderConfig);
    }
}

std::string CopyDescription::getFileTypeName(FileType fileType) {
    switch (fileType) {
    case FileType::CSV: {
        return "csv";
    }
    case FileType::PARQUET: {
        return "parquet";
    }
    case FileType::NPY: {
        return "npy";
    }
    default:
        throw InternalException("Unimplemented getFileTypeName().");
    }
}

CopyDescription::FileType CopyDescription::getFileType(std::string& fileName) {
    if (fileName.ends_with("parquet"))
        return FileType::PARQUET;
    if (fileName.ends_with("npy"))
        return FileType::NPY;
    if (fileName.ends_with("csv"))
        return FileType::CSV;
    // for "filename.csv.gz"
    auto pos = fileName.rfind('.');
    auto pos2 = fileName.rfind('.', pos - 1);
    if (pos == std::string::npos || pos2 == std::string::npos)
        return FileType::UNKNOWN;
    if (fileName.substr(pos2 + 1, 3) == "csv" &&
        (fileName.ends_with("gz") || fileName.ends_with("lzo") || fileName.ends_with("brotli") ||
            fileName.ends_with("lz4") || fileName.ends_with("zstd") || fileName.ends_with("bz2")))
        return FileType::CSV;
    return FileType::UNKNOWN;
}
} // namespace common
} // namespace kuzu
