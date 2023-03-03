#include "common/copier_config/copier_config.h"

#include "common/constants.h"
#include "common/type_utils.h"
#include "common/utils.h"
#include "spdlog/spdlog.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace common {
CopyDescription::CopyDescription(const std::string& filePath, CSVReaderConfig csvReaderConfig)
    : filePath{filePath}, csvReaderConfig{nullptr}, fileType{FileType::CSV} {
    setFileType(filePath);
    if (fileType == FileType::CSV) {
        this->csvReaderConfig = std::make_unique<CSVReaderConfig>(csvReaderConfig);
    }
}

CopyDescription::CopyDescription(const CopyDescription& copyDescription)
    : filePath{copyDescription.filePath}, csvReaderConfig{nullptr}, fileType{
                                                                        copyDescription.fileType} {
    if (fileType == FileType::CSV) {
        this->csvReaderConfig = std::make_unique<CSVReaderConfig>(*copyDescription.csvReaderConfig);
    }
}

std::string CopyDescription::getFileTypeName(FileType fileType) {
    switch (fileType) {
    case FileType::CSV:
        return "csv";

    case FileType::ARROW:
        return "arrow";

    case FileType::PARQUET:
        return "parquet";
    }
}

std::string CopyDescription::getFileTypeSuffix(FileType fileType) {
    return "." + getFileTypeName(fileType);
}

void CopyDescription::setFileType(std::string const& fileName) {
    auto csvSuffix = getFileTypeSuffix(FileType::CSV);
    auto arrowSuffix = getFileTypeSuffix(FileType::ARROW);
    auto parquetSuffix = getFileTypeSuffix(FileType::PARQUET);

    if (fileName.length() >= csvSuffix.length()) {
        if (!fileName.compare(
                fileName.length() - csvSuffix.length(), csvSuffix.length(), csvSuffix)) {
            fileType = FileType::CSV;
            return;
        }
    }

    if (fileName.length() >= arrowSuffix.length()) {
        if (!fileName.compare(
                fileName.length() - arrowSuffix.length(), arrowSuffix.length(), arrowSuffix)) {
            fileType = FileType::ARROW;
            return;
        }
    }

    if (fileName.length() >= parquetSuffix.length()) {
        if (!fileName.compare(fileName.length() - parquetSuffix.length(), parquetSuffix.length(),
                parquetSuffix)) {
            fileType = FileType::PARQUET;
            return;
        }
    }

    throw CopyException("Unsupported file type: " + fileName);
}

} // namespace common
} // namespace kuzu
