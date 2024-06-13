#include "storage/storage_utils.h"

#include <fcntl.h>

#include "common/assert.h"
#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "common/null_buffer.h"
#include "common/string_format.h"
#include "common/types/ku_list.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::string StorageUtils::getColumnName(const std::string& propertyName,
    StorageUtils::ColumnType type, const std::string& prefix) {
    switch (type) {
    case StorageUtils::ColumnType::DATA: {
        return stringFormat("{}_data", propertyName);
    }
    case StorageUtils::ColumnType::NULL_MASK: {
        return stringFormat("{}_null", propertyName);
    }
    case StorageUtils::ColumnType::INDEX: {
        return stringFormat("{}_index", propertyName);
    }
    case StorageUtils::ColumnType::OFFSET: {
        return stringFormat("{}_offset", propertyName);
    }
    case StorageUtils::ColumnType::CSR_OFFSET: {
        return stringFormat("{}_csr_offset", prefix);
    }
    case StorageUtils::ColumnType::CSR_LENGTH: {
        return stringFormat("{}_csr_length", prefix);
    }
    case StorageUtils::ColumnType::STRUCT_CHILD: {
        return stringFormat("{}_{}_child", propertyName, prefix);
    }
    default: {
        if (prefix.empty()) {
            return propertyName;
        } else {
            return stringFormat("{}_{}", prefix, propertyName);
        }
    }
    }
}

std::string StorageUtils::getNodeIndexFName(const VirtualFileSystem* vfs,
    const std::string& directory, const table_id_t& tableID, FileVersionType fileVersionType) {
    auto fName = stringFormat("n-{}", tableID);
    return appendWALFileSuffixIfNecessary(
        vfs->joinPath(directory, fName + StorageConstants::INDEX_FILE_SUFFIX), fileVersionType);
}

std::unique_ptr<FileInfo> StorageUtils::getFileInfoForReadWrite(const std::string& directory,
    DBFileID dbFileID, VirtualFileSystem* vfs) {
    std::string fName;
    switch (dbFileID.dbFileType) {
    case DBFileType::METADATA: {
        fName = getMetadataFName(vfs, directory);
    } break;
    case DBFileType::DATA: {
        fName = getDataFName(vfs, directory);
    } break;
    case DBFileType::NODE_INDEX: {
        fName = getNodeIndexFName(vfs, directory, dbFileID.nodeIndexID.tableID,
            FileVersionType::ORIGINAL);
        if (dbFileID.isOverflow) {
            fName = getOverflowFileName(fName);
        }
    } break;
    default: {
        throw RuntimeException("Unsupported dbFileID in StorageUtils::getFileInfoForReadWrite.");
    }
    }
    return vfs->openFile(fName, O_RDWR);
}

uint32_t StorageUtils::getDataTypeSize(PhysicalTypeID type) {
    switch (type) {
    case PhysicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return sizeof(ku_list_t);
    }
    case PhysicalTypeID::STRUCT: {
        // Not calculable using this interface!
        KU_UNREACHABLE;
    }
    default: {
        return PhysicalTypeUtils::getFixedTypeSize(type);
    }
    }
}

uint32_t StorageUtils::getDataTypeSize(const LogicalType& type) {
    switch (type.getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return sizeof(ku_list_t);
    }
    case PhysicalTypeID::STRUCT: {
        uint32_t size = 0;
        auto fieldsTypes = StructType::getFieldTypes(type);
        for (const auto& fieldType : fieldsTypes) {
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

std::string StorageUtils::appendSuffixOrInsertBeforeWALSuffix(const std::string& fileName,
    const std::string& suffix) {
    auto pos = fileName.find(StorageConstants::WAL_FILE_SUFFIX);
    if (pos == std::string::npos) {
        return fileName + suffix;
    } else {
        return fileName.substr(0, pos) + suffix + StorageConstants::WAL_FILE_SUFFIX;
    }
}

} // namespace storage
} // namespace kuzu
