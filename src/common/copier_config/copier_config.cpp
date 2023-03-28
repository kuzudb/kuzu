#include "common/copier_config/copier_config.h"

#include "common/type_utils.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace common {
CopyDescription::CopyDescription(const std::vector<std::string>& filePaths,
    std::unordered_map<common::property_id_t, std::string> propertyToNpyMap,
    CSVReaderConfig csvReaderConfig, FileType fileType)
    : filePaths{filePaths}, propertyIDToNpyMap{std::move(propertyToNpyMap)},
      csvReaderConfig{nullptr}, fileType{fileType} {
    if (fileType == FileType::CSV) {
        this->csvReaderConfig = std::make_unique<CSVReaderConfig>(csvReaderConfig);
    }
}

CopyDescription::CopyDescription(const CopyDescription& copyDescription)
    : filePaths{copyDescription.filePaths}, propertyIDToNpyMap{copyDescription.propertyIDToNpyMap},
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

} // namespace common
} // namespace kuzu
