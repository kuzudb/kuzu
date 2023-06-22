#include "common/copier_config/copier_config.h"

#include "common/type_utils.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace common {

// Copy From
CopyDescription::CopyDescription(
    const std::vector<std::string>& filePaths, FileType fileType, CSVReaderConfig csvReaderConfig)
    : filePaths{filePaths}, csvReaderConfig{nullptr}, fileType{fileType} {
    this->csvReaderConfig = std::make_unique<CSVReaderConfig>(csvReaderConfig);
}

// Copy To
CopyDescription::CopyDescription(const std::vector<std::string>& filePaths, FileType fileType,
    const std::vector<std::string>& columnNames)
    : filePaths{filePaths}, fileType{fileType}, columnNames{columnNames} {}

CopyDescription::CopyDescription(const CopyDescription& copyDescription)
    : filePaths{copyDescription.filePaths}, csvReaderConfig{nullptr},
      fileType{copyDescription.fileType}, columnNames{copyDescription.columnNames} {
    if (copyDescription.csvReaderConfig != nullptr) {
        this->csvReaderConfig = std::make_unique<CSVReaderConfig>(*copyDescription.csvReaderConfig);
    }
}

CopyDescription::FileType CopyDescription::getFileTypeFromExtension(const std::string& extension) {
    CopyDescription::FileType fileType = CopyDescription::fileTypeMap[extension];
    if (fileType == FileType::UNKNOWN) {
        throw CopyException("Unsupported file type " + extension);
    }
    return fileType;
}

std::string CopyDescription::getFileTypeName(FileType fileType) {
    for (const auto& fileTypeItem : fileTypeMap) {
        if (fileTypeItem.second == fileType) {
            return fileTypeItem.first;
        }
    }
    throw InternalException("Unimplemented getFileTypeName().");
}

} // namespace common
} // namespace kuzu
