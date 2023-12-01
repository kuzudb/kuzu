#include "storage/storage_utils.h"

#include "common/exception/runtime.h"
#include "common/null_buffer.h"
#include "common/string_format.h"
#include "common/types/ku_list.h"
#include "common/types/ku_string.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::string StorageUtils::getNodeIndexFName(
    const std::string& directory, const table_id_t& tableID, FileVersionType fileVersionType) {
    auto fName = stringFormat("n-{}", tableID);
    return appendWALFileSuffixIfNecessary(
        FileUtils::joinPath(directory, fName + StorageConstants::INDEX_FILE_SUFFIX),
        fileVersionType);
}

std::unique_ptr<FileInfo> StorageUtils::getFileInfoForReadWrite(
    const std::string& directory, DBFileID dbFileID) {
    std::string fName;
    switch (dbFileID.dbFileType) {
    case DBFileType::METADATA: {
        fName = getMetadataFName(directory);
    } break;
    case DBFileType::DATA: {
        fName = getDataFName(directory);
    } break;
    case DBFileType::NODE_INDEX: {
        fName =
            getNodeIndexFName(directory, dbFileID.nodeIndexID.tableID, FileVersionType::ORIGINAL);
        if (dbFileID.isOverflow) {
            fName = getOverflowFileName(fName);
        }
    } break;
    default: {
        throw RuntimeException("Unsupported dbFileID in "
                               "StorageUtils::getFileInfoFromdbFileID.");
    }
    }
    return FileUtils::openFile(fName, O_RDWR);
}

uint32_t StorageUtils::getDataTypeSize(const LogicalType& type) {
    switch (type.getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case PhysicalTypeID::FIXED_LIST: {
        return getDataTypeSize(*FixedListType::getChildType(&type)) *
               FixedListType::getNumValuesInList(&type);
    }
    case PhysicalTypeID::VAR_LIST: {
        return sizeof(ku_list_t);
    }
    case PhysicalTypeID::STRUCT: {
        uint32_t size = 0;
        auto fieldsTypes = StructType::getFieldTypes(&type);
        for (auto fieldType : fieldsTypes) {
            size += getDataTypeSize(*fieldType);
        }
        size += NullBuffer::getNumBytesForNullValues(fieldsTypes.size());
        return size;
    }
    default: {
        return PhysicalTypeUtils::getFixedTypeSize(type.getPhysicalType());
    }
    }
}

std::string StorageUtils::appendSuffixOrInsertBeforeWALSuffix(
    const std::string& fileName, const std::string& suffix) {
    auto pos = fileName.find(StorageConstants::WAL_FILE_SUFFIX);
    if (pos == std::string::npos) {
        return fileName + suffix;
    } else {
        return fileName.substr(0, pos) + suffix + StorageConstants::WAL_FILE_SUFFIX;
    }
}

} // namespace storage
} // namespace kuzu
