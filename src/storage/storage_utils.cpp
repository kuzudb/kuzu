#include "storage/storage_utils.h"

#include "common/null_buffer.h"
#include "common/string_utils.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::string StorageUtils::getNodeIndexFName(
    const std::string& directory, const table_id_t& tableID, DBFileType dbFileType) {
    auto fName = StringUtils::string_format("n-{}", tableID);
    return appendWALFileSuffixIfNecessary(
        FileUtils::joinPath(directory, fName + StorageConstants::INDEX_FILE_SUFFIX), dbFileType);
}

std::string StorageUtils::getNodePropertyColumnFName(const std::string& directory,
    const table_id_t& tableID, uint32_t propertyID, DBFileType dbFileType) {
    auto fName = StringUtils::string_format("n-{}-{}", tableID, propertyID);
    return appendWALFileSuffixIfNecessary(
        FileUtils::joinPath(directory, fName + StorageConstants::COLUMN_FILE_SUFFIX), dbFileType);
}

std::string StorageUtils::appendStructFieldName(
    std::string filePath, struct_field_idx_t structFieldIdx) {
    // Naming rules for a struct field column is: n-[tableID]-[propertyID]-[structFieldIdx].col.
    auto posToInsertFieldName = filePath.find(".col");
    filePath.insert(posToInsertFieldName, "-" + std::to_string(structFieldIdx));
    return filePath;
}

std::string StorageUtils::getPropertyNullFName(const std::string& filePath) {
    return appendSuffixOrInsertBeforeWALSuffix(filePath, ".null");
}

std::string StorageUtils::getAdjListsFName(const std::string& directory,
    const table_id_t& relTableID, const RelDataDirection& relDirection, DBFileType dbFileType) {
    auto fName = StringUtils::string_format("r-{}-{}", relTableID, relDirection);
    return appendWALFileSuffixIfNecessary(
        FileUtils::joinPath(directory, fName + StorageConstants::LISTS_FILE_SUFFIX), dbFileType);
}

std::string StorageUtils::getRelPropertyColumnFName(const std::string& directory,
    const table_id_t& relTableID, const RelDataDirection& relDirection, const uint32_t propertyID,
    DBFileType dbFileType) {
    auto fName = StringUtils::string_format("r-{}-{}-{}", relTableID, relDirection, propertyID);
    return appendWALFileSuffixIfNecessary(
        FileUtils::joinPath(directory, fName + StorageConstants::COLUMN_FILE_SUFFIX), dbFileType);
}

std::string StorageUtils::getRelPropertyListsFName(const std::string& directory,
    const table_id_t& relTableID, const RelDataDirection& relDirection, const uint32_t propertyID,
    DBFileType dbFileType) {
    auto fName = StringUtils::string_format("r-{}-{}-{}", relTableID, relDirection, propertyID);
    return appendWALFileSuffixIfNecessary(
        FileUtils::joinPath(directory, fName + StorageConstants::LISTS_FILE_SUFFIX), dbFileType);
}

std::string StorageUtils::getAdjColumnFName(const std::string& directory,
    const table_id_t& relTableID, const RelDataDirection& relDirection, DBFileType dbFileType) {
    auto fName = StringUtils::string_format("r-{}-{}", relTableID, relDirection);
    return appendWALFileSuffixIfNecessary(
        FileUtils::joinPath(directory, fName + StorageConstants::COLUMN_FILE_SUFFIX), dbFileType);
}

std::unique_ptr<FileInfo> StorageUtils::getFileInfoForReadWrite(
    const std::string& directory, StorageStructureID storageStructureID) {
    std::string fName;
    switch (storageStructureID.storageStructureType) {
    case StorageStructureType::METADATA: {
        fName = getMetadataFName(directory);
    } break;
    case StorageStructureType::DATA: {
        fName = getDataFName(directory);
    } break;
    case StorageStructureType::COLUMN: {
        fName = getColumnFName(directory, storageStructureID);
    } break;
    case StorageStructureType::LISTS: {
        fName = getListFName(directory, storageStructureID);
    } break;
    case StorageStructureType::NODE_INDEX: {
        fName = getNodeIndexFName(
            directory, storageStructureID.nodeIndexID.tableID, DBFileType::ORIGINAL);
        if (storageStructureID.isOverflow) {
            fName = getOverflowFileName(fName);
        }
    } break;
    default: {
        throw RuntimeException("Unsupported StorageStructureID in "
                               "StorageUtils::getFileInfoFromStorageStructureID.");
    }
    }
    return FileUtils::openFile(fName, O_RDWR);
}

std::string StorageUtils::getColumnFName(
    const std::string& directory, StorageStructureID storageStructureID) {
    std::string fName;
    ColumnFileID columnFileID = storageStructureID.columnFileID;
    switch (columnFileID.columnType) {
    case ColumnType::NODE_PROPERTY_COLUMN: {
        fName = getDataFName(directory);
    } break;
    case ColumnType::ADJ_COLUMN: {
        auto& relNodeTableAndDir = columnFileID.adjColumnID.relNodeTableAndDir;
        fName = getAdjColumnFName(
            directory, relNodeTableAndDir.relTableID, relNodeTableAndDir.dir, DBFileType::ORIGINAL);
        if (storageStructureID.isNullBits) {
            fName = getPropertyNullFName(fName);
        }
    } break;
    case ColumnType::REL_PROPERTY_COLUMN: {
        auto& relNodeTableAndDir = columnFileID.relPropertyColumnID.relNodeTableAndDir;
        fName = getRelPropertyColumnFName(directory, relNodeTableAndDir.relTableID,
            relNodeTableAndDir.dir, columnFileID.relPropertyColumnID.propertyID,
            DBFileType::ORIGINAL);
        if (storageStructureID.isOverflow) {
            fName = getOverflowFileName(fName);
        } else if (storageStructureID.isNullBits) {
            fName = getPropertyNullFName(fName);
        }
    } break;
    default: {
        throw NotImplementedException("StorageUtils::getColumnFName");
    }
    }
    return fName;
}

std::string StorageUtils::getListFName(
    const std::string& directory, StorageStructureID storageStructureID) {
    std::string baseFName;
    ListFileID listFileID = storageStructureID.listFileID;
    switch (listFileID.listType) {
    case ListType::ADJ_LISTS: {
        auto& relNodeTableAndDir = listFileID.adjListsID.relNodeTableAndDir;
        baseFName = getAdjListsFName(
            directory, relNodeTableAndDir.relTableID, relNodeTableAndDir.dir, DBFileType::ORIGINAL);
    } break;
    case ListType::REL_PROPERTY_LISTS: {
        auto& relNodeTableAndDir = listFileID.relPropertyListID.relNodeTableAndDir;
        baseFName = getRelPropertyListsFName(directory, relNodeTableAndDir.relTableID,
            relNodeTableAndDir.dir, listFileID.relPropertyListID.propertyID, DBFileType::ORIGINAL);
    } break;
    default:
        throw NotImplementedException("StorageUtils::getListFName listType");
    }

    switch (listFileID.listFileType) {
    case ListFileType::BASE_LISTS:
        if (storageStructureID.isOverflow) {
            return StorageUtils::getOverflowFileName(baseFName);
        } else {
            return baseFName;
        }
    case ListFileType::HEADERS: {
        return getListHeadersFName(baseFName);
    }
    case ListFileType::METADATA: {
        return getListMetadataFName(baseFName);
    }
    default:
        throw NotImplementedException("StorageUtils::getListFName listFileType");
    }
}

void StorageUtils::createFileForRelPropertyWithDefaultVal(RelTableSchema* tableSchema,
    const Property& property, uint8_t* defaultVal, bool isDefaultValNull,
    StorageManager& storageManager) {
    for (auto direction : RelDataDirectionUtils::getRelDataDirections()) {
        auto createPropertyFileFunc = tableSchema->isSingleMultiplicityInDirection(direction) ?
                                          createFileForRelColumnPropertyWithDefaultVal :
                                          createFileForRelListsPropertyWithDefaultVal;
        createPropertyFileFunc(tableSchema->tableID, tableSchema->getBoundTableID(direction),
            direction, property, defaultVal, isDefaultValNull, storageManager);
    }
}

void StorageUtils::createFileForRelColumnPropertyWithDefaultVal(table_id_t relTableID,
    table_id_t boundTableID, RelDataDirection direction, const catalog::Property& property,
    uint8_t* defaultVal, bool isDefaultValNull, StorageManager& storageManager) {
    auto inMemColumn = std::make_unique<InMemColumn>(
        StorageUtils::getRelPropertyColumnFName(storageManager.getDirectory(), relTableID,
            direction, property.getPropertyID(), DBFileType::WAL_VERSION),
        *property.getDataType());
    auto numTuples =
        storageManager.getRelsStore().getRelsStatistics().getNumTuplesForTable(relTableID);
    auto inMemColumnChunk =
        inMemColumn->createInMemColumnChunk(0, numTuples - 1, nullptr /* copyDescription */);
    if (!isDefaultValNull) {
        // TODO(Guodong): Rework this.
        //        inMemColumn->fillWithDefaultVal(defaultVal,
        //            storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs().getNumTuplesForTable(
        //                boundTableID),
        //            property.dataType);
    }
    inMemColumn->flushChunk(inMemColumnChunk.get());
    inMemColumn->saveToFile();
}

void StorageUtils::createFileForRelListsPropertyWithDefaultVal(table_id_t relTableID,
    table_id_t boundTableID, RelDataDirection direction, const catalog::Property& property,
    uint8_t* defaultVal, bool isDefaultValNull, StorageManager& storageManager) {
    // Note: we need the listMetadata to get the num of elements in a large list, and headers to
    // get the num of elements in a small list as well as determine whether a list is large or
    // small. All property lists share the same listHeader which is stored in the adjList.
    auto adjLists = storageManager.getRelsStore().getAdjLists(direction, relTableID);
    auto inMemList = InMemListsFactory::getInMemPropertyLists(
        StorageUtils::getRelPropertyListsFName(storageManager.getDirectory(), relTableID, direction,
            property.getPropertyID(), DBFileType::WAL_VERSION),
        *property.getDataType(),
        storageManager.getRelsStore().getRelsStatistics().getNumTuplesForTable(relTableID),
        nullptr /* copyDescription */);
    auto numNodesInBoundTable =
        storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs().getNumTuplesForTable(
            boundTableID);
    inMemList->initListsMetadataAndAllocatePages(
        numNodesInBoundTable, adjLists->getHeaders().get(), &adjLists->getListsMetadata());
    if (!isDefaultValNull) {
        inMemList->fillWithDefaultVal(
            defaultVal, numNodesInBoundTable, adjLists->getHeaders().get());
    }
    inMemList->saveToFile();
}

uint32_t StorageUtils::getDataTypeSize(const LogicalType& type) {
    switch (type.getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case PhysicalTypeID::FIXED_LIST: {
        return getDataTypeSize(*FixedListType::getChildType(&type)) *
               FixedListType::getNumElementsInList(&type);
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
    std::string fileName, std::string suffix) {
    auto pos = fileName.find(StorageConstants::WAL_FILE_SUFFIX);
    if (pos == std::string::npos) {
        return fileName + suffix;
    } else {
        return fileName.substr(0, pos) + suffix + StorageConstants::WAL_FILE_SUFFIX;
    }
}

void StorageUtils::initializeListsHeaders(table_id_t relTableID, uint64_t numNodesInTable,
    const std::string& directory, RelDataDirection relDirection) {
    auto listHeadersBuilder = make_unique<ListHeadersBuilder>(
        StorageUtils::getAdjListsFName(directory, relTableID, relDirection, DBFileType::ORIGINAL),
        numNodesInTable);
    listHeadersBuilder->saveToDisk();
}

} // namespace storage
} // namespace kuzu
