#include "common/copier_config/copier_config.h"

#include "common/type_utils.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace common {

CopyDescription::CopyDescription(
    const std::vector<std::string>& filePaths, CSVReaderConfig csvReaderConfig)
    : filePaths{filePaths}, csvReaderConfig{nullptr} {
    inferFileType();
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

void CopyDescription::inferFileType() {
    assert(!filePaths.empty());
    // We currently only support loading from files with the same type. Loading files with
    // different types is not supported.
    auto fileName = filePaths[0];
    auto csvSuffix = CopyDescription::getFileTypeSuffix(CopyDescription::FileType::CSV);
    auto arrowSuffix = CopyDescription::getFileTypeSuffix(CopyDescription::FileType::ARROW);
    auto parquetSuffix = CopyDescription::getFileTypeSuffix(CopyDescription::FileType::PARQUET);

    if (fileName.length() >= csvSuffix.length()) {
        if (!fileName.compare(
                fileName.length() - csvSuffix.length(), csvSuffix.length(), csvSuffix)) {
            fileType = CopyDescription::FileType::CSV;
            return;
        }
    }

    if (fileName.length() >= arrowSuffix.length()) {
        if (!fileName.compare(
                fileName.length() - arrowSuffix.length(), arrowSuffix.length(), arrowSuffix)) {
            fileType = CopyDescription::FileType::ARROW;
            return;
        }
    }

    if (fileName.length() >= parquetSuffix.length()) {
        if (!fileName.compare(fileName.length() - parquetSuffix.length(), parquetSuffix.length(),
                parquetSuffix)) {
            fileType = CopyDescription::FileType::PARQUET;
            return;
        }
    }

    throw CopyException("Unsupported file type: " + fileName);
}

std::string CopyDescription::getFileTypeName(FileType fileType) {
    switch (fileType) {
    case FileType::CSV: {
        return "csv";
    }
    case FileType::ARROW: {
        return "arrow";
    }
    case FileType::PARQUET: {
        return "parquet";
    }
    }
}

} // namespace common
} // namespace kuzu
