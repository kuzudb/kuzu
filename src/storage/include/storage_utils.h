#pragma once

#include <string>

#include "src/catalog/include/catalog_structs.h"
#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/null_mask.h"
#include "src/common/include/utils.h"
#include "src/common/types/include/types_include.h"
#include "src/storage/wal/include/wal_record.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

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
        : pageIdx{pageIdx}, posInPage{posInPage} {};
    PageElementCursor() : PageElementCursor{UINT32_MAX, UINT16_MAX} {};

    inline void nextPage() {
        pageIdx++;
        posInPage = 0;
    }

    page_idx_t pageIdx;
    uint16_t posInPage;
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
    inline static unique_ptr<FileInfo> getFileInfoForReadWrite(
        const string& directory, StorageStructureID storageStructureID) {
        string fName;
        switch (storageStructureID.storageStructureType) {
        case STRUCTURED_NODE_PROPERTY_COLUMN: {
            fName = getNodePropertyColumnFName(directory,
                storageStructureID.structuredNodePropertyColumnID.nodeLabel,
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

    inline static string getNodeIndexFName(
        const string& directory, const label_t& nodeLabel, DBFileType dbFileType) {
        auto fName = StringUtils::string_format("n-%d", nodeLabel);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::INDEX_FILE_SUFFIX), dbFileType);
    }

    inline static string getNodePropertyColumnFName(const string& directory,
        const label_t& nodeLabel, uint32_t propertyID, DBFileType dbFileType) {
        auto fName = StringUtils::string_format("n-%d-%d", nodeLabel, propertyID);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX), dbFileType);
    }

    inline static StorageStructureIDAndFName getStructuredNodePropertyColumnStructureIDAndFName(
        const string& directory, const catalog::Property& property) {
        auto fName = getNodePropertyColumnFName(
            directory, property.label, property.propertyID, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(StorageStructureID::newStructuredNodePropertyMainColumnID(
                                              property.label, property.propertyID),
            fName);
    }

    inline static StorageStructureIDAndFName getNodeIndexIDAndFName(
        const string& directory, label_t nodeLabel) {
        auto fName = getNodeIndexFName(directory, nodeLabel, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(StorageStructureID::newNodeIndexID(nodeLabel), fName);
    }

    // Returns the StorageStructureIDAndFName for the "base" lists structure/file. Callers need to
    // modify it to obtain versions for METADATA and HEADERS structures/files.
    inline static StorageStructureIDAndFName getUnstructuredNodePropertyListsStructureIDAndFName(
        const string& directory, label_t nodeLabel) {
        auto fName = getNodeUnstrPropertyListsFName(directory, nodeLabel, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(StorageStructureID::newUnstructuredNodePropertyListsID(
                                              nodeLabel, ListFileType::BASE_LISTS),
            fName);
    }

    // Returns the StorageStructureIDAndFName for the "base" lists structure/file. Callers need to
    // modify it to obtain versions for METADATA and HEADERS structures/files.
    inline static StorageStructureIDAndFName getAdjListsStructureIDAndFName(
        const string& directory, label_t relLabel, label_t srcNodeLabel, RelDirection dir) {
        auto fName = getAdjListsFName(directory, relLabel, srcNodeLabel, dir, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(StorageStructureID::newAdjListsID(relLabel, srcNodeLabel,
                                              dir, ListFileType::BASE_LISTS),
            fName);
    }

    // Returns the StorageStructureIDAndFName for the "base" lists structure/file. Callers need to
    // modify it to obtain versions for METADATA and HEADERS structures/files.
    inline static StorageStructureIDAndFName getRelPropertyListsStructureIDAndFName(
        const string& directory, label_t relLabel, label_t srcNodeLabel, RelDirection dir,
        const catalog::Property& property) {
        auto fName = getRelPropertyListsFName(
            directory, relLabel, srcNodeLabel, dir, property.propertyID, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(
            StorageStructureID::newRelPropertyListsID(
                relLabel, srcNodeLabel, dir, property.propertyID, ListFileType::BASE_LISTS),
            fName);
    }

    inline static string getNodeUnstrPropertyListsFName(
        const string& directory, const label_t& nodeLabel, DBFileType dbFileType) {
        auto fName = StringUtils::string_format("n-%d", nodeLabel);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX), dbFileType);
    }

    inline static string getAdjColumnFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const RelDirection& relDirection, DBFileType dbFileType) {
        auto fName = StringUtils::string_format("r-%d-%d-%d", relLabel, nodeLabel, relDirection);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX), dbFileType);
    }

    // TODO(Semih): Warning: We currently do not support updates on adj columns or their
    // properties so we construct a dummy and wrong StorageStructureIDAndFName here. Later we should
    // create a special file names for these and use the correct one. But the fName we construct
    // is correct.
    inline static StorageStructureIDAndFName getAdjColumnStructureIDAndFName(
        const string& directory, const label_t& relLabel, const label_t& nodeLabel,
        const RelDirection& relDirection) {
        auto fName =
            getAdjColumnFName(directory, relLabel, nodeLabel, relDirection, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(
            StorageStructureID::newStructuredNodePropertyMainColumnID(nodeLabel, -1), fName);
    }

    inline static string getAdjListsFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const RelDirection& relDirection, DBFileType dbFileType) {
        auto fName = StringUtils::string_format("r-%d-%d-%d", relLabel, nodeLabel, relDirection);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX), dbFileType);
    }

    inline static string getRelPropertyColumnFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const RelDirection& relDirection, const string& propertyName,
        DBFileType dbFileType) {
        auto fName = StringUtils::string_format(
            "r-%d-%d-%d-%s", relLabel, nodeLabel, relDirection, propertyName.data());
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX), dbFileType);
    }
    // TODO(Semih): Warning: We currently do not support updates on adj columns or their
    // properties so we construct a dummy and wrong StorageStructureIDAndFName here. Later we should
    // create a special file names for these and use the correct one. But the fName we construct
    // is correct.
    inline static StorageStructureIDAndFName getRelPropertyColumnStructureIDAndFName(
        const string& directory, const label_t& relLabel, const label_t& nodeLabel,
        const RelDirection& relDirection, const string& propertyName) {
        auto fName = getRelPropertyColumnFName(
            directory, relLabel, nodeLabel, relDirection, propertyName, DBFileType::ORIGINAL);
        return StorageStructureIDAndFName(
            StorageStructureID::newStructuredNodePropertyMainColumnID(nodeLabel, -1), fName);
    }

    inline static string getRelPropertyListsFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const RelDirection& relDirection, const uint32_t propertyID,
        DBFileType dbFileType) {
        auto fName = StringUtils::string_format(
            "r-%d-%d-%d-%d", relLabel, nodeLabel, relDirection, propertyID);
        return appendWALFileSuffixIfNecessary(
            FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX), dbFileType);
    }

    inline static string getListHeadersFName(string baseListFName) {
        return appendSuffixOrInsertBeforeWALSuffix(baseListFName, ".headers");
    }

    inline static string getListMetadataFName(string baseListFName) {
        return appendSuffixOrInsertBeforeWALSuffix(baseListFName, ".metadata");
    }

    inline static string getListFName(
        const string& directory, StorageStructureID storageStructureID) {
        string baseFName;
        ListFileID listFileID = storageStructureID.listFileID;
        switch (listFileID.listType) {
        case UNSTRUCTURED_NODE_PROPERTY_LISTS: {
            baseFName = getNodeUnstrPropertyListsFName(directory,
                listFileID.unstructuredNodePropertyListsID.nodeLabel, DBFileType::ORIGINAL);
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

    inline static string getOverflowPagesFName(const string& fName) {
        return appendSuffixOrInsertBeforeWALSuffix(fName, StorageConfig::OVERFLOW_FILE_SUFFIX);
    }

    static inline void overwriteNodesMetadataFileWithVersionFromWAL(const string& directory) {
        FileUtils::overwriteFile(getNodesMetadataFilePath(directory, DBFileType::WAL_VERSION),
            getNodesMetadataFilePath(directory, DBFileType::ORIGINAL));
    }

    static inline string getNodesMetadataFilePath(const string& directory, DBFileType dbFileType) {
        return FileUtils::joinPath(
            directory, dbFileType == DBFileType::ORIGINAL ?
                           common::StorageConfig::NODES_METADATA_FILE_NAME :
                           common::StorageConfig::NODES_METADATA_FILE_NAME_FOR_WAL);
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
    inline static pair<uint64_t, uint64_t> getQuotientRemainder(uint64_t i, uint64_t divisor) {
        return make_pair(i / divisor, i % divisor);
    }

    inline static void removeAllWALFiles(const string& directory) {
        for (auto& folderIter : filesystem::directory_iterator(directory)) {
            if (folderIter.path().extension() == StorageConfig::WAL_FILE_SUFFIX) {
                filesystem::remove(folderIter.path());
            }
        }
    }

    inline static string appendWALFileSuffixIfNecessary(string fileName, DBFileType dbFileType) {
        return dbFileType == DBFileType::WAL_VERSION ? appendWALFileSuffix(fileName) : fileName;
    }

    inline static string appendWALFileSuffix(string fileName) {
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
