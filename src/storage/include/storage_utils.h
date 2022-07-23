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
                storageStructureID.structuredNodePropertyColumnID.propertyID);
            if (storageStructureID.isOverflow) {
                fName = getOverflowPagesFName(fName);
            }
        } break;
        case LISTS: {
            fName = getListFName(directory, storageStructureID.listFileID);
        } break;
        case NODE_INDEX: {
            throw RuntimeException("There should not be any code path yet triggering getting "
                                   "NODE_INDEX file name from StorageStructureID.");
        } break;
        default: {
            throw RuntimeException("Unsupported StorageStructureID in "
                                   "StorageUtils::getFileInfoFromStorageStructureID.");
        }
        }
        return FileUtils::openFile(fName, O_RDWR);
    }

    inline static string getNodeIndexFName(const string& directory, const label_t& nodeLabel) {
        auto fName = StringUtils::string_format("n-%d", nodeLabel);
        return FileUtils::joinPath(directory, fName + StorageConfig::INDEX_FILE_SUFFIX);
    }
    inline static string getNodePropertyColumnFName(
        const string& directory, const label_t& nodeLabel, uint32_t propertyID) {
        auto fName = StringUtils::string_format("n-%d-%d", nodeLabel, propertyID);
        return FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX);
    }

    inline static StorageStructureIDAndFName getStructuredNodePropertyColumnStructureIDAndFName(
        const string& directory, const catalog::Property& property) {
        auto fName = getNodePropertyColumnFName(directory, property.label, property.propertyID);
        return StorageStructureIDAndFName(StorageStructureID::newStructuredNodePropertyMainColumnID(
                                              property.label, property.propertyID),
            fName);
    }

    inline static StorageStructureIDAndFName getNodeIndexIDAndFName(
        const string& directory, label_t nodeLabel) {
        auto fName = getNodeIndexFName(directory, nodeLabel);
        return StorageStructureIDAndFName(StorageStructureID::newNodeIndexID(nodeLabel), fName);
    }

    // Returns the StorageStructureIDAndFName for the "base" lists structure/file. Callers need to
    // modify it to obtain versions for METADATA and HEADERS structures/files.
    inline static StorageStructureIDAndFName getUnstructuredNodePropertyListsStructureIDAndFName(
        const string& directory, label_t nodeLabel) {
        auto fName = getNodeUnstrPropertyListsFName(directory, nodeLabel);
        return StorageStructureIDAndFName(StorageStructureID::newUnstructuredNodePropertyListsID(
                                              nodeLabel, ListFileType::BASE_LISTS),
            fName);
    }

    // Returns the StorageStructureIDAndFName for the "base" lists structure/file. Callers need to
    // modify it to obtain versions for METADATA and HEADERS structures/files.
    inline static StorageStructureIDAndFName getAdjListsStructureIDAndFName(
        const string& directory, label_t relLabel, label_t srcNodeLabel, RelDirection dir) {
        auto fName = getAdjListsFName(directory, relLabel, srcNodeLabel, dir);
        return StorageStructureIDAndFName(StorageStructureID::newAdjListsID(relLabel, srcNodeLabel,
                                              dir, ListFileType::BASE_LISTS),
            fName);
    }

    // Returns the StorageStructureIDAndFName for the "base" lists structure/file. Callers need to
    // modify it to obtain versions for METADATA and HEADERS structures/files.
    inline static StorageStructureIDAndFName getRelPropertyListsStructureIDAndFName(
        const string& directory, label_t relLabel, label_t srcNodeLabel, RelDirection dir,
        const catalog::Property& property) {
        auto fName =
            getRelPropertyListsFName(directory, relLabel, srcNodeLabel, dir, property.propertyID);
        return StorageStructureIDAndFName(
            StorageStructureID::newRelPropertyListsID(
                relLabel, srcNodeLabel, dir, property.propertyID, ListFileType::BASE_LISTS),
            fName);
    }

    inline static string getNodeUnstrPropertyListsFName(
        const string& directory, const label_t& nodeLabel) {
        auto fName = StringUtils::string_format("n-%d", nodeLabel);
        return FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX);
    }

    inline static string getAdjColumnFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const RelDirection& relDirection) {
        auto fName = StringUtils::string_format("r-%d-%d-%d", relLabel, nodeLabel, relDirection);
        return FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX);
    }

    // TODO(Semih): Warning: We currently do not support updates on adj columns or their
    // properties so we construct a dummy and wrong StorageStructureIDAndFName here. Later we should
    // create a special file names for these and use the correct one. But the fName we construct
    // is correct.
    inline static StorageStructureIDAndFName getAdjColumnStructureIDAndFName(
        const string& directory, const label_t& relLabel, const label_t& nodeLabel,
        const RelDirection& relDirection) {
        auto fName = getAdjColumnFName(directory, relLabel, nodeLabel, relDirection);
        return StorageStructureIDAndFName(
            StorageStructureID::newStructuredNodePropertyMainColumnID(nodeLabel, -1), fName);
    }

    inline static string getAdjListsFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const RelDirection& relDirection) {
        auto fName = StringUtils::string_format("r-%d-%d-%d", relLabel, nodeLabel, relDirection);
        return FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX);
    }

    inline static string getRelPropertyColumnFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const RelDirection& relDirection, const string& propertyName) {
        auto fName = StringUtils::string_format(
            "r-%d-%d-%d-%s", relLabel, nodeLabel, relDirection, propertyName.data());
        return FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX);
    }
    // TODO(Semih): Warning: We currently do not support updates on adj columns or their
    // properties so we construct a dummy and wrong StorageStructureIDAndFName here. Later we should
    // create a special file names for these and use the correct one. But the fName we construct
    // is correct.
    inline static StorageStructureIDAndFName getRelPropertyColumnStructureIDAndFName(
        const string& directory, const label_t& relLabel, const label_t& nodeLabel,
        const RelDirection& relDirection, const string& propertyName) {
        auto fName =
            getRelPropertyColumnFName(directory, relLabel, nodeLabel, relDirection, propertyName);
        return StorageStructureIDAndFName(
            StorageStructureID::newStructuredNodePropertyMainColumnID(nodeLabel, -1), fName);
    }

    inline static string getRelPropertyListsFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const RelDirection& relDirection, const uint32_t propertyID) {
        auto fName = StringUtils::string_format(
            "r-%d-%d-%d-%d", relLabel, nodeLabel, relDirection, propertyID);
        return FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX);
    }

    inline static string getListHeadersFName(string baseListFName) {
        return baseListFName + ".headers";
    }

    inline static string getListFName(const string& directory, ListFileID listFileID) {
        string baseFName;
        switch (listFileID.listType) {
        case UNSTRUCTURED_NODE_PROPERTY_LISTS: {
            baseFName = getNodeUnstrPropertyListsFName(
                directory, listFileID.unstructuredNodePropertyListsID.nodeLabel);
        } break;
        default: {
            throw RuntimeException(
                "There should not be any code path yet triggering getting List "
                "file name for anything other than UnstructuredNodePropertyList ListType.");
        }
            //            , ADJ_LISTS = 1, REL_PROPERTY_LISTS = 2,
        }

        switch (listFileID.listFileType) {
        case HEADERS: {
            return getListHeadersFName(baseFName);
        }
        default: {
            throw RuntimeException("There should not be any code path yet triggering getting List "
                                   "file name for anything other than a HEADERS ListFileType.");
        }
        }
    }

    inline static string getOverflowPagesFName(const string& fName) {
        return fName + StorageConfig::OVERFLOW_FILE_SUFFIX;
    }

    static inline void removeNodesMetadataFileForWALIfExists(const string& directory) {
        FileUtils::removeFile(getNodesMetadataFilePath(directory, true /* isForWALRecord */));
    }

    static inline void overwriteNodesMetadataFileWithVersionFromWAL(const string& directory) {
        FileUtils::overwriteFile(getNodesMetadataFilePath(directory, true /* isForWALRecord */),
            getNodesMetadataFilePath(directory, false /* original nodes metadata file */));
    }

    static inline string getNodesMetadataFilePath(const string& directory, bool isForWALRecord) {
        return FileUtils::joinPath(
            directory, isForWALRecord ? common::StorageConfig::NODES_METADATA_FILE_NAME_FOR_WAL :
                                        common::StorageConfig::NODES_METADATA_FILE_NAME);
    }

    static inline uint64_t getListChunkIdx(node_offset_t nodeOffset) {
        return nodeOffset >> StorageConfig::LISTS_CHUNK_SIZE_LOG_2;
    }

    static inline void removeCatalogFileForWALIfExists(const string& directory) {
        FileUtils::removeFile(getCatalogFilePath(directory, true /* isForWALRecord */));
    }

    static inline void overwriteCatalogFileWithVersionFromWAL(const string& directory) {
        FileUtils::overwriteFile(getCatalogFilePath(directory, true /* isForWALRecord */),
            getCatalogFilePath(directory, false /* original catalog file */));
    }

    static inline string getCatalogFilePath(const string& directory, bool isForWALRecord) {
        return FileUtils::joinPath(directory, isForWALRecord ?
                                                  common::StorageConfig::CATALOG_FILE_NAME_FOR_WAL :
                                                  common::StorageConfig::CATALOG_FILE_NAME);
    }

    // Note: This is a relatively slow function because of division and mod and making pair. It is
    // not meant to be used in performance critical code path.
    inline static pair<uint64_t, uint64_t> getQuotientRemainder(uint64_t i, uint64_t divisor) {
        return make_pair(i / divisor, i % divisor);
    }
};

} // namespace storage
} // namespace graphflow
