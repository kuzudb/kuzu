#include "storage/storage_utils.h"

#include "common/string_utils.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::string StorageUtils::getNodeIndexFName(const std::string& directory,
    const common::table_id_t& tableID, common::DBFileType dbFileType) {
    auto fName = StringUtils::string_format("n-{}", tableID);
    return appendWALFileSuffixIfNecessary(
        common::FileUtils::joinPath(directory, fName + common::StorageConstants::INDEX_FILE_SUFFIX),
        dbFileType);
}

std::string StorageUtils::getNodePropertyColumnFName(const std::string& directory,
    const common::table_id_t& tableID, uint32_t propertyID, common::DBFileType dbFileType) {
    auto fName = common::StringUtils::string_format("n-{}-{}", tableID, propertyID);
    return appendWALFileSuffixIfNecessary(common::FileUtils::joinPath(directory,
                                              fName + common::StorageConstants::COLUMN_FILE_SUFFIX),
        dbFileType);
}

std::string StorageUtils::getAdjListsFName(const std::string& directory,
    const common::table_id_t& relTableID, const common::RelDirection& relDirection,
    common::DBFileType dbFileType) {
    auto fName = common::StringUtils::string_format("r-{}-{}", relTableID, relDirection);
    return appendWALFileSuffixIfNecessary(
        common::FileUtils::joinPath(directory, fName + common::StorageConstants::LISTS_FILE_SUFFIX),
        dbFileType);
}

std::string StorageUtils::getRelPropertyColumnFName(const std::string& directory,
    const common::table_id_t& relTableID, const common::RelDirection& relDirection,
    const uint32_t propertyID, common::DBFileType dbFileType) {
    auto fName =
        common::StringUtils::string_format("r-{}-{}-{}", relTableID, relDirection, propertyID);
    return appendWALFileSuffixIfNecessary(common::FileUtils::joinPath(directory,
                                              fName + common::StorageConstants::COLUMN_FILE_SUFFIX),
        dbFileType);
}

std::string StorageUtils::getRelPropertyListsFName(const std::string& directory,
    const common::table_id_t& relTableID, const common::RelDirection& relDirection,
    const uint32_t propertyID, common::DBFileType dbFileType) {
    auto fName =
        common::StringUtils::string_format("r-{}-{}-{}", relTableID, relDirection, propertyID);
    return appendWALFileSuffixIfNecessary(
        common::FileUtils::joinPath(directory, fName + common::StorageConstants::LISTS_FILE_SUFFIX),
        dbFileType);
}

std::string StorageUtils::getAdjColumnFName(const std::string& directory,
    const common::table_id_t& relTableID, const common::RelDirection& relDirection,
    common::DBFileType dbFileType) {
    auto fName = common::StringUtils::string_format("r-{}-{}", relTableID, relDirection);
    return appendWALFileSuffixIfNecessary(common::FileUtils::joinPath(directory,
                                              fName + common::StorageConstants::COLUMN_FILE_SUFFIX),
        dbFileType);
}

std::unique_ptr<FileInfo> StorageUtils::getFileInfoForReadWrite(
    const std::string& directory, StorageStructureID storageStructureID) {
    std::string fName;
    switch (storageStructureID.storageStructureType) {
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
        fName = getNodePropertyColumnFName(directory,
            storageStructureID.columnFileID.nodePropertyColumnID.tableID,
            storageStructureID.columnFileID.nodePropertyColumnID.propertyID, DBFileType::ORIGINAL);
        if (storageStructureID.isOverflow) {
            fName = getOverflowFileName(fName);
        }
    } break;
    case ColumnType::ADJ_COLUMN: {
        auto& relNodeTableAndDir = columnFileID.adjColumnID.relNodeTableAndDir;
        fName = getAdjColumnFName(
            directory, relNodeTableAndDir.relTableID, relNodeTableAndDir.dir, DBFileType::ORIGINAL);
    } break;
    case ColumnType::REL_PROPERTY_COLUMN: {
        auto& relNodeTableAndDir = columnFileID.relPropertyColumnID.relNodeTableAndDir;
        fName = getRelPropertyColumnFName(directory, relNodeTableAndDir.relTableID,
            relNodeTableAndDir.dir, columnFileID.relPropertyColumnID.propertyID,
            DBFileType::ORIGINAL);
        if (storageStructureID.isOverflow) {
            fName = getOverflowFileName(fName);
        }
    } break;
    default: {
        assert(false);
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
        assert(false);
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
        assert(false);
    }
}

void StorageUtils::createFileForNodePropertyWithDefaultVal(table_id_t tableID,
    const std::string& directory, const catalog::Property& property, uint8_t* defaultVal,
    bool isDefaultValNull, uint64_t numNodes) {
    auto inMemColumn = InMemColumnFactory::getInMemPropertyColumn(
        StorageUtils::getNodePropertyColumnFName(
            directory, tableID, property.propertyID, DBFileType::WAL_VERSION),
        property.dataType, numNodes);
    if (!isDefaultValNull) {
        inMemColumn->fillWithDefaultVal(defaultVal, numNodes, property.dataType);
    }
    inMemColumn->saveToFile();
}

void StorageUtils::createFileForRelPropertyWithDefaultVal(RelTableSchema* tableSchema,
    const Property& property, uint8_t* defaultVal, bool isDefaultValNull,
    StorageManager& storageManager) {
    for (auto direction : REL_DIRECTIONS) {
        auto createPropertyFileFunc = tableSchema->isSingleMultiplicityInDirection(direction) ?
                                          createFileForRelColumnPropertyWithDefaultVal :
                                          createFileForRelListsPropertyWithDefaultVal;
        createPropertyFileFunc(tableSchema->tableID, tableSchema->getBoundTableID(direction),
            direction, property, defaultVal, isDefaultValNull, storageManager);
    }
}

void StorageUtils::createFileForRelColumnPropertyWithDefaultVal(table_id_t relTableID,
    table_id_t boundTableID, RelDirection direction, const catalog::Property& property,
    uint8_t* defaultVal, bool isDefaultValNull, StorageManager& storageManager) {
    auto inMemColumn = InMemColumnFactory::getInMemPropertyColumn(
        StorageUtils::getRelPropertyColumnFName(storageManager.getDirectory(), relTableID,
            direction, property.propertyID, DBFileType::WAL_VERSION),
        property.dataType,
        storageManager.getRelsStore().getRelsStatistics().getNumTuplesForTable(relTableID));
    if (!isDefaultValNull) {
        inMemColumn->fillWithDefaultVal(defaultVal,
            storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs().getNumTuplesForTable(
                boundTableID),
            property.dataType);
    }
    inMemColumn->saveToFile();
}

void StorageUtils::createFileForRelListsPropertyWithDefaultVal(table_id_t relTableID,
    table_id_t boundTableID, RelDirection direction, const catalog::Property& property,
    uint8_t* defaultVal, bool isDefaultValNull, StorageManager& storageManager) {
    auto inMemList = InMemListsFactory::getInMemPropertyLists(
        StorageUtils::getRelPropertyListsFName(storageManager.getDirectory(), relTableID, direction,
            property.propertyID, DBFileType::WAL_VERSION),
        property.dataType,
        storageManager.getRelsStore().getRelsStatistics().getNumTuplesForTable(relTableID));
    // Note: we need the listMetadata to get the num of elements in a large list, and headers to
    // get the num of elements in a small list as well as determine whether a list is large or
    // small. All property lists share the same listHeader which is stored in the adjList.
    auto adjLists = storageManager.getRelsStore().getAdjLists(direction, relTableID);
    auto numNodesInBoundTable =
        storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs().getNumTuplesForTable(
            boundTableID);
    inMemList->initListsMetadataAndAllocatePages(
        numNodesInBoundTable, adjLists->getHeaders().get(), &adjLists->getListsMetadata());
    if (!isDefaultValNull) {
        inMemList->fillWithDefaultVal(
            defaultVal, numNodesInBoundTable, adjLists, property.dataType);
    }
    inMemList->saveToFile();
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

uint32_t PageUtils::getNumElementsInAPage(uint32_t elementSize, bool hasNull) {
    auto numBytesPerNullEntry = NullMask::NUM_BITS_PER_NULL_ENTRY >> 3;
    auto numNullEntries =
        hasNull ? (uint32_t)ceil(
                      (double)BufferPoolConstants::PAGE_4KB_SIZE /
                      (double)(((uint64_t)elementSize << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2) +
                               numBytesPerNullEntry)) :
                  0;
    return (BufferPoolConstants::PAGE_4KB_SIZE - (numNullEntries * numBytesPerNullEntry)) /
           elementSize;
}

void StorageUtils::initializeListsHeaders(const RelTableSchema* relTableSchema,
    uint64_t numNodesInTable, const std::string& directory, RelDirection relDirection) {
    auto listHeadersBuilder = make_unique<ListHeadersBuilder>(
        StorageUtils::getAdjListsFName(
            directory, relTableSchema->tableID, relDirection, DBFileType::WAL_VERSION),
        numNodesInTable);
    listHeadersBuilder->saveToDisk();
}

} // namespace storage
} // namespace kuzu
