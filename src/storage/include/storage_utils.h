#pragma once

#include <string>

#include "src/catalog/include/catalog_structs.h"
#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/null_mask.h"
#include "src/common/include/utils.h"
#include "src/common/types/include/types_include.h"
#include "src/storage/wal/include/wal_record.h"

namespace graphflow {
namespace storage {

using namespace graphflow::common;

struct StorageStructureIDAndFName {
    StorageStructureIDAndFName(StorageStructureID storageStructureID, string fName)
        : storageStructureID{storageStructureID}, fName{move(fName)} {};

    StorageStructureIDAndFName(const StorageStructureIDAndFName& other)
        : storageStructureID{other.storageStructureID}, fName{other.fName} {};

    StorageStructureID storageStructureID;
    string fName;
};

struct PageByteCursor {

    PageByteCursor(page_idx_t pageIdx, uint16_t offsetInPage)
        : pageIdx{pageIdx}, offsetInPage{offsetInPage} {};
    PageByteCursor() : PageByteCursor{UINT32_MAX, UINT16_MAX} {};

    page_idx_t pageIdx;
    uint16_t offsetInPage;
};

struct PageElementCursor {

    PageElementCursor(page_idx_t pageIdx, uint16_t posInPage)
        : pageIdx{pageIdx}, elemPosInPage{posInPage} {};
    PageElementCursor() : PageElementCursor{UINT32_MAX, UINT16_MAX} {};

    inline void nextPage() {
        pageIdx++;
        elemPosInPage = 0;
    }

    page_idx_t pageIdx;
    uint16_t elemPosInPage;
};

struct PageUtils {

    static uint32_t getNumElementsInAPage(uint32_t elementSize, bool hasNull);

    // This function returns the page pageIdx of the page where element will be found and the pos of
    // the element in the page as the offset.
    static inline PageElementCursor getPageElementCursorForPos(
        const uint64_t& elementPos, const uint32_t numElementsPerPage) {
        assert((elementPos / numElementsPerPage) < UINT32_MAX);
        return PageElementCursor{(page_idx_t)(elementPos / numElementsPerPage),
            (uint16_t)(elementPos % numElementsPerPage)};
    }
};

class StorageUtils {

public:
    static inline unique_ptr<FileInfo> getFileInfoForReadWrite(
        const string& directory, StorageStructureID storageStructureID) {
        string fName;
        switch (storageStructureID.storageStructureType) {
        case STRUCTURED_NODE_PROPERTY_COLUMN: {
            fName = getNodePropertyColumnFName(directory,
                storageStructureID.structuredNodePropertyColumnID.tableID,
                storageStructureID.structuredNodePropertyColumnID.propertyID, DBFileType::ORIGINAL);
            if (storageStructureID.isOverflow) {
                fName = getOverflowPagesFName(fName);
            }
        } break;
        case LISTS: {
            fName = getListFName(directory, storageStructureID);
        } break;
        case NODE_INDEX: {
            throw RuntimeException("There should not be any code path yet triggering getting "
                                   "NODE_INDEX file name from StorageStructureID.");
        }
        default: {
            throw RuntimeException("Unsupported StorageStructureID in "
                                   "StorageUtils::getFileInfoFromStorageStructureID.");
        }
        }
        return FileUtils::openFile(fName, O_RDWR);
    }

    static inline string getNodeIndexFName(
        const string& directory, const table_id_t& tableID, DBFileType dbFileType) {
        auto fName = StringUtils::string_format("n-%d", tableID);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::INDEX_FILE_SUFFIX), dbFileType);
    }

    static inline string getNodePropertyColumnFName(const string& directory,
        const table_id_t& tableID, uint32_t propertyID, DBFileType dbFileType) {
        auto fName = StringUtils::string_format("n-%d-%d", tableID, propertyID);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX), dbFileType);
    }

    static inline StorageStructureIDAndFName getStructuredNodePropertyColumnStructureIDAndFName(
        const string& directory, const catalog::Property& property) {
        auto fName = getNodePropertyColumnFName(
            directory, property.tableID, property.propertyID, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(StorageStructureID::newStructuredNodePropertyMainColumnID(
                                              property.tableID, property.propertyID),
            fName);
    }

    static inline StorageStructureIDAndFName getNodeIndexIDAndFName(
        const string& directory, table_id_t tableID) {
        auto fName = getNodeIndexFName(directory, tableID, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(StorageStructureID::newNodeIndexID(tableID), fName);
    }

    // Returns the StorageStructureIDAndFName for the "base" lists structure/file. Callers need to
    // modify it to obtain versions for METADATA and HEADERS structures/files.
    static inline StorageStructureIDAndFName getUnstructuredNodePropertyListsStructureIDAndFName(
        const string& directory, table_id_t tableID) {
        auto fName = getNodeUnstrPropertyListsFName(directory, tableID, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(StorageStructureID::newUnstructuredNodePropertyListsID(
                                              tableID, ListFileType::BASE_LISTS),
            fName);
    }

    // Returns the StorageStructureIDAndFName for the "base" lists structure/file. Callers need to
    // modify it to obtain versions for METADATA and HEADERS structures/files.
    static inline StorageStructureIDAndFName getAdjListsStructureIDAndFName(const string& directory,
        table_id_t relTableID, table_id_t srcNodeTableID, RelDirection dir) {
        auto fName =
            getAdjListsFName(directory, relTableID, srcNodeTableID, dir, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(StorageStructureID::newAdjListsID(relTableID,
                                              srcNodeTableID, dir, ListFileType::BASE_LISTS),
            fName);
    }

    // Returns the StorageStructureIDAndFName for the "base" lists structure/file. Callers need to
    // modify it to obtain versions for METADATA and HEADERS structures/files.
    static inline StorageStructureIDAndFName getRelPropertyListsStructureIDAndFName(
        const string& directory, table_id_t relTableID, table_id_t srcNodeTableID, RelDirection dir,
        const catalog::Property& property) {
        auto fName = getRelPropertyListsFName(
            directory, relTableID, srcNodeTableID, dir, property.propertyID, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(
            StorageStructureID::newRelPropertyListsID(
                relTableID, srcNodeTableID, dir, property.propertyID, ListFileType::BASE_LISTS),
            fName);
    }

    static inline string getNodeUnstrPropertyListsFName(
        const string& directory, const table_id_t& tableID, DBFileType dbFileType) {
        auto fName = StringUtils::string_format("n-%d", tableID);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX), dbFileType);
    }

    static inline string getAdjColumnFName(const string& directory, const table_id_t& relTableID,
        const table_id_t& nodeTableID, const RelDirection& relDirection, DBFileType dbFileType) {
        auto fName =
            StringUtils::string_format("r-%d-%d-%d", relTableID, nodeTableID, relDirection);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX), dbFileType);
    }

    // TODO(Semih): Warning: We currently do not support updates on adj columns or their
    // properties so we construct a dummy and wrong StorageStructureIDAndFName here. Later we should
    // create a special file names for these and use the correct one. But the fName we construct
    // is correct.
    static inline StorageStructureIDAndFName getAdjColumnStructureIDAndFName(
        const string& directory, const table_id_t& relTableID, const table_id_t& nodeTableID,
        const RelDirection& relDirection) {
        auto fName = getAdjColumnFName(
            directory, relTableID, nodeTableID, relDirection, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(
            StorageStructureID::newStructuredNodePropertyMainColumnID(nodeTableID, -1), fName);
    }

    static inline string getAdjListsFName(const string& directory, const table_id_t& relTableID,
        const table_id_t& nodeTableID, const RelDirection& relDirection, DBFileType dbFileType) {
        auto fName =
            StringUtils::string_format("r-%d-%d-%d", relTableID, nodeTableID, relDirection);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX), dbFileType);
    }

    static inline string getRelPropertyColumnFName(const string& directory,
        const table_id_t& relTableID, const table_id_t& nodeTableID,
        const RelDirection& relDirection, const string& propertyName, DBFileType dbFileType) {
        auto fName = StringUtils::string_format(
            "r-%d-%d-%d-%s", relTableID, nodeTableID, relDirection, propertyName.data());
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX), dbFileType);
    }
    // TODO(Semih): Warning: We currently do not support updates on adj columns or their
    // properties so we construct a dummy and wrong StorageStructureIDAndFName here. Later we should
    // create a special file names for these and use the correct one. But the fName we construct
    // is correct.
    static inline StorageStructureIDAndFName getRelPropertyColumnStructureIDAndFName(
        const string& directory, const table_id_t& relTableID, const table_id_t& nodeTableID,
        const RelDirection& relDirection, const string& propertyName) {
        auto fName = getRelPropertyColumnFName(
            directory, relTableID, nodeTableID, relDirection, propertyName, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(
            StorageStructureID::newStructuredNodePropertyMainColumnID(nodeTableID, -1), fName);
    }

    static inline string getRelPropertyListsFName(const string& directory,
        const table_id_t& relTableID, const table_id_t& nodeTableID,
        const RelDirection& relDirection, const uint32_t propertyID, DBFileType dbFileType) {
        auto fName = StringUtils::string_format(
            "r-%d-%d-%d-%d", relTableID, nodeTableID, relDirection, propertyID);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX), dbFileType);
    }

    static inline string getListHeadersFName(string baseListFName) {
        return appendSuffixOrInsertBeforeWALSuffix(baseListFName, ".headers");
    }

    static inline string getListMetadataFName(string baseListFName) {
        return appendSuffixOrInsertBeforeWALSuffix(baseListFName, ".metadata");
    }

    static inline string getListFName(
        const string& directory, StorageStructureID storageStructureID) {
        string baseFName;
        ListFileID listFileID = storageStructureID.listFileID;
        switch (listFileID.listType) {
        case UNSTRUCTURED_NODE_PROPERTY_LISTS: {
            baseFName = getNodeUnstrPropertyListsFName(directory,
                listFileID.unstructuredNodePropertyListsID.tableID, DBFileType::ORIGINAL);
        } break;
        case ADJ_LISTS: {
            auto relNodeTableAndDir = listFileID.adjListsID.relNodeTableAndDir;
            baseFName = getAdjListsFName(directory, relNodeTableAndDir.relTableID,
                relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.dir, DBFileType::ORIGINAL);
        } break;
        case REL_PROPERTY_LISTS: {
            auto& relNodeTableAndDir = listFileID.relPropertyListID.relNodeTableAndDir;
            baseFName = getRelPropertyListsFName(directory, relNodeTableAndDir.relTableID,
                relNodeTableAndDir.srcNodeTableID, relNodeTableAndDir.dir,
                listFileID.relPropertyListID.propertyID, DBFileType::ORIGINAL);
        } break;
        default: {
            throw RuntimeException(
                "There should not be any code path yet triggering getting List "
                "file name for anything other than UnstructuredNodePropertyList ListType.");
        }
        }

        switch (listFileID.listFileType) {
        case BASE_LISTS:
            if (storageStructureID.isOverflow) {
                return StorageUtils::getOverflowPagesFName(baseFName);
            } else {
                return baseFName;
            }
        case HEADERS: {
            return getListHeadersFName(baseFName);
        }
        case METADATA: {
            return getListMetadataFName(baseFName);
        }
        default: {
            throw RuntimeException("This should never happen. There are no listFileType other than "
                                   "BASE_LISTS, HEADERS, and METADATA");
        }
        }
    }

    static inline string getOverflowPagesFName(const string& fName) {
        return appendSuffixOrInsertBeforeWALSuffix(fName, StorageConfig::OVERFLOW_FILE_SUFFIX);
    }

    static inline void overwriteNodesStatisticsAndDeletedIDsFileWithVersionFromWAL(
        const string& directory) {
        FileUtils::overwriteFile(
            getNodesStatisticsAndDeletedIDsFilePath(directory, DBFileType::WAL_VERSION),
            getNodesStatisticsAndDeletedIDsFilePath(directory, DBFileType::ORIGINAL));
    }

    static inline string getNodesStatisticsAndDeletedIDsFilePath(
        const string& directory, DBFileType dbFileType) {
        return FileUtils::joinPath(
            directory, dbFileType == DBFileType::ORIGINAL ?
                           common::StorageConfig::NODES_STATISTICS_AND_DELETED_IDS_FILE_NAME :
                           common::StorageConfig::NODES_STATISTICS_FILE_NAME_FOR_WAL);
    }

    static inline void overwriteRelsStatisticsFileWithVersionFromWAL(const string& directory) {
        FileUtils::overwriteFile(getRelsStatisticsFilePath(directory, DBFileType::WAL_VERSION),
            getRelsStatisticsFilePath(directory, DBFileType::ORIGINAL));
    }

    static inline string getRelsStatisticsFilePath(const string& directory, DBFileType dbFileType) {
        return FileUtils::joinPath(
            directory, dbFileType == DBFileType::ORIGINAL ?
                           common::StorageConfig::RELS_METADATA_FILE_NAME :
                           common::StorageConfig::RELS_METADATA_FILE_NAME_FOR_WAL);
    }

    static inline uint64_t getNumChunks(node_offset_t numNodes) {
        auto numChunks = StorageUtils::getListChunkIdx(numNodes);
        if (0 != (numNodes & (ListsMetadataConfig::LISTS_CHUNK_SIZE - 1))) {
            numChunks++;
        }
        return numChunks;
    }

    static inline uint64_t getListChunkIdx(node_offset_t nodeOffset) {
        return nodeOffset >> ListsMetadataConfig::LISTS_CHUNK_SIZE_LOG_2;
    }

    static inline node_offset_t getChunkIdxBeginNodeOffset(uint64_t chunkIdx) {
        return chunkIdx << ListsMetadataConfig::LISTS_CHUNK_SIZE_LOG_2;
    }

    static inline node_offset_t getChunkIdxEndNodeOffsetInclusive(uint64_t chunkIdx) {
        return ((chunkIdx + 1) << ListsMetadataConfig::LISTS_CHUNK_SIZE_LOG_2) - 1;
    }

    static inline void overwriteCatalogFileWithVersionFromWAL(const string& directory) {
        FileUtils::overwriteFile(getCatalogFilePath(directory, DBFileType::WAL_VERSION),
            getCatalogFilePath(directory, DBFileType::ORIGINAL));
    }

    static inline string getCatalogFilePath(const string& directory, DBFileType dbFileType) {
        return FileUtils::joinPath(directory, dbFileType == DBFileType::ORIGINAL ?
                                                  common::StorageConfig::CATALOG_FILE_NAME :
                                                  common::StorageConfig::CATALOG_FILE_NAME_FOR_WAL);
    }

    // Note: This is a relatively slow function because of division and mod and making pair.
    // It is not meant to be used in performance critical code path.
    static inline pair<uint64_t, uint64_t> getQuotientRemainder(uint64_t i, uint64_t divisor) {
        return make_pair(i / divisor, i % divisor);
    }

    static inline void removeAllWALFiles(const string& directory) {
        for (auto& folderIter : filesystem::directory_iterator(directory)) {
            if (folderIter.path().extension() == StorageConfig::WAL_FILE_SUFFIX) {
                filesystem::remove(folderIter.path());
            }
        }
    }

    static inline string appendWALFileSuffixIfNecessary(string fileName, DBFileType dbFileType) {
        return dbFileType == DBFileType::WAL_VERSION ? appendWALFileSuffix(fileName) : fileName;
    }

    static inline string appendWALFileSuffix(string fileName) {
        assert(fileName.find(StorageConfig::WAL_FILE_SUFFIX) == string::npos);
        return fileName + StorageConfig::WAL_FILE_SUFFIX;
    }

private:
    inline static string appendSuffixOrInsertBeforeWALSuffix(string fileName, string suffix) {
        auto pos = fileName.find(StorageConfig::WAL_FILE_SUFFIX);
        if (pos == string::npos) {
            return fileName + suffix;
        } else {
            return fileName.substr(0, pos) + suffix + StorageConfig::WAL_FILE_SUFFIX;
        }
    }
};

} // namespace storage
} // namespace graphflow
