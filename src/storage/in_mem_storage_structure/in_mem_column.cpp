#include "storage/in_mem_storage_structure/in_mem_column.h"

#include "common/constants.h"
#include "storage/file_handle.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InMemColumn::InMemColumn(std::string filePath, LogicalType dataType, bool requireNullBits)
    : filePath{std::move(filePath)}, dataType{std::move(dataType)} {
    // TODO(Guodong): Separate this as a function.
    switch (this->dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        auto fieldTypes = common::StructType::getFieldTypes(&this->dataType);
        childColumns.reserve(fieldTypes.size());
        for (auto i = 0u; i < fieldTypes.size(); i++) {
            childColumns.push_back(std::make_unique<InMemColumn>(
                StorageUtils::appendStructFieldName(this->filePath, i), *fieldTypes[i],
                true /* hasNull */));
        }
    } break;
    case PhysicalTypeID::STRING:
    case PhysicalTypeID::VAR_LIST: {
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
                LogicalType(LogicalTypeID::BOOL), false /* hasNull */);
    }
}

void InMemColumn::flushChunk(InMemColumnChunk* chunk) {
    if (fileHandle) {
        auto fileInfo = fileHandle->getFileInfo();
        chunk->flush(fileInfo);
    }
    if (!childColumns.empty()) {
        auto inMemStructColumnChunk = reinterpret_cast<InMemStructColumnChunk*>(chunk);
        for (auto i = 0u; i < childColumns.size(); i++) {
            childColumns[i]->flushChunk(inMemStructColumnChunk->getFieldChunk(i));
        }
    }
    if (nullColumn) {
        nullColumn->flushChunk(chunk->getNullChunk());
    }
}

void InMemColumn::saveToFile() {
    if (inMemOverflowFile) {
        inMemOverflowFile->flush();
    }
    for (auto& column : childColumns) {
        column->saveToFile();
    }
}

} // namespace storage
} // namespace kuzu
