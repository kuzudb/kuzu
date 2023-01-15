#include "storage/storage_utils.h"

#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace storage {

unique_ptr<FileInfo> StorageUtils::getFileInfoForReadWrite(
    const string& directory, StorageStructureID storageStructureID) {
    string fName;
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

string StorageUtils::getColumnFName(
    const string& directory, StorageStructureID storageStructureID) {
    string fName;
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
        fName = getAdjColumnFName(directory, relNodeTableAndDir.relTableID,
            relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.dir, DBFileType::ORIGINAL);
    } break;
    case ColumnType::REL_PROPERTY_COLUMN: {
        auto& relNodeTableAndDir = columnFileID.relPropertyColumnID.relNodeTableAndDir;
        fName = getRelPropertyColumnFName(directory, relNodeTableAndDir.relTableID,
            relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.dir,
            columnFileID.relPropertyColumnID.propertyID, DBFileType::ORIGINAL);
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

string StorageUtils::getListFName(const string& directory, StorageStructureID storageStructureID) {
    string baseFName;
    ListFileID listFileID = storageStructureID.listFileID;
    switch (listFileID.listType) {
    case ListType::ADJ_LISTS: {
        auto& relNodeTableAndDir = listFileID.adjListsID.relNodeTableAndDir;
        baseFName = getAdjListsFName(directory, relNodeTableAndDir.relTableID,
            relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.dir, DBFileType::ORIGINAL);
    } break;
    case ListType::REL_PROPERTY_LISTS: {
        auto& relNodeTableAndDir = listFileID.relPropertyListID.relNodeTableAndDir;
        baseFName = getRelPropertyListsFName(directory, relNodeTableAndDir.relTableID,
            relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.dir,
            listFileID.relPropertyListID.propertyID, DBFileType::ORIGINAL);
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
    const string& directory, const catalog::Property& property, uint8_t* defaultVal,
    bool isDefaultValNull, uint64_t numNodes) {
    auto inMemColumn = InMemColumnFactory::getInMemPropertyColumn(
        StorageUtils::getNodePropertyColumnFName(
            directory, tableID, property.propertyID, DBFileType::ORIGINAL),
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
        for (auto boundTableID : tableSchema->getUniqueBoundTableIDs(direction)) {
            createPropertyFileFunc(tableSchema->tableID, boundTableID, direction, property,
                defaultVal, isDefaultValNull, storageManager);
        }
    }
}

void StorageUtils::createFileForRelColumnPropertyWithDefaultVal(table_id_t relTableID,
    table_id_t boundTableID, RelDirection direction, const catalog::Property& property,
    uint8_t* defaultVal, bool isDefaultValNull, StorageManager& storageManager) {
    auto inMemColumn = InMemColumnFactory::getInMemPropertyColumn(
        StorageUtils::getRelPropertyColumnFName(storageManager.getDirectory(), relTableID,
            boundTableID, direction, property.propertyID, DBFileType::ORIGINAL),
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
        StorageUtils::getRelPropertyListsFName(storageManager.getDirectory(), relTableID,
            boundTableID, direction, property.propertyID, DBFileType::ORIGINAL),
        property.dataType,
        storageManager.getRelsStore().getRelsStatistics().getNumTuplesForTable(relTableID));
    // Note: we need the listMetadata to get the num of elements in a large list, and headers to
    // get the num of elements in a small list as well as determine whether a list is large or
    // small. All property lists share the same listHeader which is stored in the adjList.
    auto adjLists = storageManager.getRelsStore().getAdjLists(direction, boundTableID, relTableID);
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

string StorageUtils::appendSuffixOrInsertBeforeWALSuffix(string fileName, string suffix) {
    auto pos = fileName.find(StorageConfig::WAL_FILE_SUFFIX);
    if (pos == string::npos) {
        return fileName + suffix;
    } else {
        return fileName.substr(0, pos) + suffix + StorageConfig::WAL_FILE_SUFFIX;
    }
}

uint32_t PageUtils::getNumElementsInAPage(uint32_t elementSize, bool hasNull) {
    auto numBytesPerNullEntry = NullMask::NUM_BITS_PER_NULL_ENTRY >> 3;
    auto numNullEntries =
        hasNull ? (uint32_t)ceil(
                      (double)DEFAULT_PAGE_SIZE /
                      (double)(((uint64_t)elementSize << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2) +
                               numBytesPerNullEntry)) :
                  0;
    return (DEFAULT_PAGE_SIZE - (numNullEntries * numBytesPerNullEntry)) / elementSize;
}

} // namespace storage
} // namespace kuzu
