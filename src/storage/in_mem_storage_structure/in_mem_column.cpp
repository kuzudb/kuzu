#include "storage/in_mem_storage_structure/in_mem_column.h"

#include "common/constants.h"
#include "storage/file_handle.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InMemColumn::InMemColumn(std::string filePath, DataType dataType, bool requireNullBits)
    : filePath{std::move(filePath)},
      numBytesForValue{(uint16_t)common::Types::getDataTypeSize(dataType)}, dataType{std::move(
                                                                                dataType)} {
    // TODO(Guodong): Separate this as a function.
    switch (this->dataType.typeID) {
    case STRUCT: {
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(this->dataType.getExtraTypeInfo());
        auto childTypes = structTypeInfo->getChildrenTypes();
        auto childNames = structTypeInfo->getChildrenNames();
        childColumns.resize(childTypes.size());
        for (auto i = 0u; i < childTypes.size(); i++) {
            childColumns[i] = std::make_unique<InMemColumn>(
                StorageUtils::appendStructFieldName(this->filePath, childNames[i]), *childTypes[i],
                true /* hasNull */);
        }
    } break;
    case STRING:
    case VAR_LIST: {
        inMemOverflowFile =
            std::make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(this->filePath));
        fileHandle = std::make_unique<FileHandle>(
            this->filePath, FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    } break;
    default: {
        fileHandle = std::make_unique<FileHandle>(
            this->filePath, FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    }
    }
    if (requireNullBits) {
        nullColumn =
            std::make_unique<InMemColumn>(StorageUtils::getPropertyNullFName(this->filePath),
                DataType(BOOL), false /* hasNull */);
    }
}

void InMemColumn::flushChunk(InMemColumnChunk* chunk) {
    if (fileHandle) {
        auto fileInfo = fileHandle->getFileInfo();
        chunk->flush(fileInfo);
    }
    for (auto i = 0u; i < childColumns.size(); i++) {
        childColumns[i]->flushChunk(chunk->getChildChunk(i));
    }
    if (nullColumn) {
        nullColumn->flushChunk(chunk->getNullChunk());
    }
}

void InMemColumn::saveToFile() {
    if (inMemOverflowFile) {
        inMemOverflowFile->flush();
    }
}

} // namespace storage
} // namespace kuzu
