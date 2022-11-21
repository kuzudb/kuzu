#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

unique_ptr<FileInfo> StorageUtils::getFileInfoForReadWrite(
    const string& directory, StorageStructureID storageStructureID) {
    string fName;
    switch (storageStructureID.storageStructureType) {
    case COLUMN: {
        fName = getColumnFName(directory, storageStructureID);
    } break;
    case LISTS: {
        fName = getListFName(directory, storageStructureID);
    } break;
    case NODE_INDEX: {
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
    case STRUCTURED_NODE_PROPERTY_COLUMN: {
        fName = getNodePropertyColumnFName(directory,
            storageStructureID.columnFileID.structuredNodePropertyColumnID.tableID,
            storageStructureID.columnFileID.structuredNodePropertyColumnID.propertyID,
            DBFileType::ORIGINAL);
        if (storageStructureID.isOverflow) {
            fName = getOverflowFileName(fName);
        }
    } break;
    case ADJ_COLUMN: {
        auto& relNodeTableAndDir = columnFileID.adjColumnID.relNodeTableAndDir;
        fName = getAdjColumnFName(directory, relNodeTableAndDir.relTableID,
            relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.dir, DBFileType::ORIGINAL);
    } break;
    case REL_PROPERTY_COLUMN: {
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
    case ADJ_LISTS: {
        auto& relNodeTableAndDir = listFileID.adjListsID.relNodeTableAndDir;
        baseFName = getAdjListsFName(directory, relNodeTableAndDir.relTableID,
            relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.dir, DBFileType::ORIGINAL);
    } break;
    case REL_PROPERTY_LISTS: {
        auto& relNodeTableAndDir = listFileID.relPropertyListID.relNodeTableAndDir;
        baseFName = getRelPropertyListsFName(directory, relNodeTableAndDir.relTableID,
            relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.dir,
            listFileID.relPropertyListID.propertyID, DBFileType::ORIGINAL);
    } break;
    default:
        assert(false);
    }

    switch (listFileID.listFileType) {
    case BASE_LISTS:
        if (storageStructureID.isOverflow) {
            return StorageUtils::getOverflowFileName(baseFName);
        } else {
            return baseFName;
        }
    case HEADERS: {
        return getListHeadersFName(baseFName);
    }
    case METADATA: {
        return getListMetadataFName(baseFName);
    }
    default:
        assert(false);
    }
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
