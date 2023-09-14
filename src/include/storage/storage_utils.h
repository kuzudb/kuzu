#pragma once

#include <string>
#include <utility>

#include "catalog/table_schema.h"
#include "common/constants.h"
#include "common/file_utils.h"
#include "common/null_mask.h"
#include "common/utils.h"
#include "storage/wal/wal_record.h"

namespace kuzu {
namespace storage {

class StorageManager;

struct StorageStructureIDAndFName {
    StorageStructureIDAndFName(StorageStructureID storageStructureID, std::string fName)
        : storageStructureID{storageStructureID}, fName{std::move(fName)} {};
    StorageStructureID storageStructureID;
    std::string fName;
};

struct PageByteCursor {
    PageByteCursor(common::page_idx_t pageIdx, uint16_t offsetInPage)
        : pageIdx{pageIdx}, offsetInPage{offsetInPage} {};
    PageByteCursor() : PageByteCursor{UINT32_MAX, UINT16_MAX} {};

    inline void resetValue() {
        pageIdx = UINT32_MAX;
        offsetInPage = UINT16_MAX;
    }

    common::page_idx_t pageIdx;
    uint16_t offsetInPage;
};

struct PageElementCursor {
    PageElementCursor(common::page_idx_t pageIdx, uint16_t posInPage)
        : pageIdx{pageIdx}, elemPosInPage{posInPage} {};
    PageElementCursor() : PageElementCursor{UINT32_MAX, UINT16_MAX} {};

    inline void nextPage() {
        pageIdx++;
        elemPosInPage = 0;
    }

    common::page_idx_t pageIdx;
    uint16_t elemPosInPage;
};

struct PageUtils {
    static constexpr uint32_t getNumElementsInAPage(uint32_t elementSize, bool hasNull) {
        assert(elementSize > 0);
        auto numBytesPerNullEntry = common::NullMask::NUM_BITS_PER_NULL_ENTRY >> 3;
        auto numNullEntries =
            hasNull ?
                (uint32_t)ceil((double)common::BufferPoolConstants::PAGE_4KB_SIZE /
                               (double)(((uint64_t)elementSize
                                            << common::NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2) +
                                        numBytesPerNullEntry)) :
                0;
        return (common::BufferPoolConstants::PAGE_4KB_SIZE -
                   (numNullEntries * numBytesPerNullEntry)) /
               elementSize;
    }

    // This function returns the page pageIdx of the page where element will be found and the pos of
    // the element in the page as the offset.
    static inline PageElementCursor getPageElementCursorForPos(
        uint64_t elementPos, uint32_t numElementsPerPage) {
        assert((elementPos / numElementsPerPage) < UINT32_MAX);
        return PageElementCursor{(common::page_idx_t)(elementPos / numElementsPerPage),
            (uint16_t)(elementPos % numElementsPerPage)};
    }

    static inline PageByteCursor getPageByteCursorForPos(
        uint64_t elementPos, uint32_t numElementsPerPage, uint64_t elementSize) {
        assert((elementPos / numElementsPerPage) < UINT32_MAX);
        return PageByteCursor{(common::page_idx_t)(elementPos / numElementsPerPage),
            (uint16_t)(elementPos % numElementsPerPage * elementSize)};
    }
};

class StorageUtils {
public:
    static inline common::offset_t getStartOffsetOfNodeGroup(
        common::node_group_idx_t nodeGroupIdx) {
        return nodeGroupIdx << common::StorageConstants::NODE_GROUP_SIZE_LOG2;
    }
    static inline common::offset_t getStartOffsetOfVectorInChunk(common::vector_idx_t vectorIdx) {
        return vectorIdx << common::DEFAULT_VECTOR_CAPACITY_LOG_2;
    }
    static inline common::node_group_idx_t getNodeGroupIdx(common::offset_t nodeOffset) {
        return nodeOffset >> common::StorageConstants::NODE_GROUP_SIZE_LOG2;
    }

    static inline common::vector_idx_t getVectorIdx(common::offset_t offsetInChunk) {
        return offsetInChunk >> common::DEFAULT_VECTOR_CAPACITY_LOG_2;
    }
    static inline common::vector_idx_t getVectorIdxInChunk(
        common::offset_t nodeOffset, common::node_group_idx_t nodeGroupIdx) {
        return (nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx)) >>
               common::DEFAULT_VECTOR_CAPACITY_LOG_2;
    }
    static inline std::pair<common::vector_idx_t, common::offset_t>
    getVectorIdxInChunkAndOffsetInVector(
        common::offset_t nodeOffset, common::node_group_idx_t nodeGroupIdx) {
        auto startOffsetOfNodeGroup = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        auto offsetInChunk = nodeOffset - startOffsetOfNodeGroup;
        auto vectorIdx = getVectorIdx(offsetInChunk);
        return std::make_pair(vectorIdx, offsetInChunk - getStartOffsetOfVectorInChunk(vectorIdx));
    }

    static std::string getNodeIndexFName(const std::string& directory,
        const common::table_id_t& tableID, common::DBFileType dbFileType);

    static inline std::string getDataFName(const std::string& directory) {
        return common::FileUtils::joinPath(directory, common::StorageConstants::DATA_FILE_NAME);
    }

    static inline std::string getMetadataFName(const std::string& directory) {
        return common::FileUtils::joinPath(directory, common::StorageConstants::METADATA_FILE_NAME);
    }

    // TODO: This function should be removed after the refactoring of rel tables into node groups.
    static std::string getNodePropertyColumnFName(const std::string& directory,
        const common::table_id_t& tableID, uint32_t propertyID, common::DBFileType dbFileType);

    static std::string appendStructFieldName(
        std::string filePath, common::struct_field_idx_t structFieldIdx);
    static std::string getPropertyNullFName(const std::string& filePath);

    static inline StorageStructureIDAndFName getNodeNullColumnStructureIDAndFName(
        StorageStructureIDAndFName propertyColumnIDAndFName) {
        auto nullColumnStructureIDAndFName = propertyColumnIDAndFName;
        nullColumnStructureIDAndFName.fName =
            StorageUtils::getPropertyNullFName(propertyColumnIDAndFName.fName);
        nullColumnStructureIDAndFName.storageStructureID.isNullBits = true;
        return nullColumnStructureIDAndFName;
    }

    static inline StorageStructureIDAndFName getNodeIndexIDAndFName(
        const std::string& directory, common::table_id_t tableID) {
        auto fName = getNodeIndexFName(directory, tableID, common::DBFileType::ORIGINAL);
        return {StorageStructureID::newNodeIndexID(tableID), fName};
    }

    // Returns the StorageStructureIDAndFName for the "base" lists structure/file. Callers need to
    // modify it to obtain versions for METADATA and HEADERS structures/files.
    static inline StorageStructureIDAndFName getAdjListsStructureIDAndFName(
        const std::string& directory, common::table_id_t relTableID, common::RelDataDirection dir) {
        auto fName = getAdjListsFName(directory, relTableID, dir, common::DBFileType::ORIGINAL);
        return {
            StorageStructureID::newAdjListsID(relTableID, dir, ListFileType::BASE_LISTS), fName};
    }

    // Returns the StorageStructureIDAndFName for the "base" lists structure/file. Callers need to
    // modify it to obtain versions for METADATA and HEADERS structures/files.
    static inline StorageStructureIDAndFName getRelPropertyListsStructureIDAndFName(
        const std::string& directory, common::table_id_t relTableID, common::RelDataDirection dir,
        const catalog::Property& property) {
        auto fName = getRelPropertyListsFName(
            directory, relTableID, dir, property.getPropertyID(), common::DBFileType::ORIGINAL);
        return {StorageStructureID::newRelPropertyListsID(
                    relTableID, dir, property.getPropertyID(), ListFileType::BASE_LISTS),
            fName};
    }

    static std::string getAdjColumnFName(const std::string& directory,
        const common::table_id_t& relTableID, const common::RelDataDirection& relDirection,
        common::DBFileType dbFileType);

    static inline StorageStructureIDAndFName getAdjColumnStructureIDAndFName(
        const std::string& directory, const common::table_id_t& relTableID,
        const common::RelDataDirection& relDirection) {
        auto fName =
            getAdjColumnFName(directory, relTableID, relDirection, common::DBFileType::ORIGINAL);
        return {StorageStructureID::newAdjColumnID(relTableID, relDirection), fName};
    }

    static std::string getAdjListsFName(const std::string& directory,
        const common::table_id_t& relTableID, const common::RelDataDirection& relDirection,
        common::DBFileType dbFileType);

    static std::string getRelPropertyColumnFName(const std::string& directory,
        const common::table_id_t& relTableID, const common::RelDataDirection& relDirection,
        uint32_t propertyID, common::DBFileType dbFileType);

    static inline StorageStructureIDAndFName getRelPropertyColumnStructureIDAndFName(
        const std::string& directory, const common::table_id_t& relTableID,
        const common::RelDataDirection& relDirection, uint32_t propertyID) {
        auto fName = getRelPropertyColumnFName(
            directory, relTableID, relDirection, propertyID, common::DBFileType::ORIGINAL);
        return {StorageStructureID::newRelPropertyColumnID(relTableID, relDirection, propertyID),
            fName};
    }

    static std::string getRelPropertyListsFName(const std::string& directory,
        const common::table_id_t& relTableID, const common::RelDataDirection& relDirection,
        uint32_t propertyID, common::DBFileType dbFileType);

    static inline std::string getListHeadersFName(std::string baseListFName) {
        return appendSuffixOrInsertBeforeWALSuffix(std::move(baseListFName), ".headers");
    }

    static inline std::string getListMetadataFName(std::string baseListFName) {
        return appendSuffixOrInsertBeforeWALSuffix(std::move(baseListFName), ".metadata");
    }

    static inline std::string getOverflowFileName(const std::string& fName) {
        return appendSuffixOrInsertBeforeWALSuffix(
            fName, common::StorageConstants::OVERFLOW_FILE_SUFFIX);
    }

    static inline void overwriteNodesStatisticsAndDeletedIDsFileWithVersionFromWAL(
        const std::string& directory) {
        common::FileUtils::overwriteFile(
            getNodesStatisticsAndDeletedIDsFilePath(directory, common::DBFileType::WAL_VERSION),
            getNodesStatisticsAndDeletedIDsFilePath(directory, common::DBFileType::ORIGINAL));
    }

    static inline std::string getNodesStatisticsAndDeletedIDsFilePath(
        const std::string& directory, common::DBFileType dbFileType) {
        return common::FileUtils::joinPath(
            directory, dbFileType == common::DBFileType::ORIGINAL ?
                           common::StorageConstants::NODES_STATISTICS_AND_DELETED_IDS_FILE_NAME :
                           common::StorageConstants::NODES_STATISTICS_FILE_NAME_FOR_WAL);
    }

    static inline void overwriteRelsStatisticsFileWithVersionFromWAL(const std::string& directory) {
        common::FileUtils::overwriteFile(
            getRelsStatisticsFilePath(directory, common::DBFileType::WAL_VERSION),
            getRelsStatisticsFilePath(directory, common::DBFileType::ORIGINAL));
    }

    static inline std::string getRelsStatisticsFilePath(
        const std::string& directory, common::DBFileType dbFileType) {
        return common::FileUtils::joinPath(
            directory, dbFileType == common::DBFileType::ORIGINAL ?
                           common::StorageConstants::RELS_METADATA_FILE_NAME :
                           common::StorageConstants::RELS_METADATA_FILE_NAME_FOR_WAL);
    }

    static inline uint64_t getNumChunks(common::offset_t numNodes) {
        auto numChunks = StorageUtils::getListChunkIdx(numNodes);
        if (0 != (numNodes & (common::ListsMetadataConstants::LISTS_CHUNK_SIZE - 1))) {
            numChunks++;
        }
        return numChunks;
    }

    static inline uint64_t getListChunkIdx(common::offset_t nodeOffset) {
        return nodeOffset >> common::ListsMetadataConstants::LISTS_CHUNK_SIZE_LOG_2;
    }

    static inline common::offset_t getChunkIdxBeginNodeOffset(uint64_t chunkIdx) {
        return chunkIdx << common::ListsMetadataConstants::LISTS_CHUNK_SIZE_LOG_2;
    }

    static inline common::offset_t getChunkIdxEndNodeOffsetInclusive(uint64_t chunkIdx) {
        return ((chunkIdx + 1) << common::ListsMetadataConstants::LISTS_CHUNK_SIZE_LOG_2) - 1;
    }

    static inline void overwriteCatalogFileWithVersionFromWAL(const std::string& directory) {
        common::FileUtils::overwriteFile(
            getCatalogFilePath(directory, common::DBFileType::WAL_VERSION),
            getCatalogFilePath(directory, common::DBFileType::ORIGINAL));
    }

    static inline std::string getCatalogFilePath(
        const std::string& directory, common::DBFileType dbFileType) {
        return common::FileUtils::joinPath(
            directory, dbFileType == common::DBFileType::ORIGINAL ?
                           common::StorageConstants::CATALOG_FILE_NAME :
                           common::StorageConstants::CATALOG_FILE_NAME_FOR_WAL);
    }

    // Note: This is a relatively slow function because of division and mod and making std::pair.
    // It is not meant to be used in performance critical code path.
    static inline std::pair<uint64_t, uint64_t> getQuotientRemainder(uint64_t i, uint64_t divisor) {
        return std::make_pair(i / divisor, i % divisor);
    }

    static inline void removeAllWALFiles(const std::string& directory) {
        for (auto& folderIter : std::filesystem::directory_iterator(directory)) {
            if (folderIter.path().extension() == common::StorageConstants::WAL_FILE_SUFFIX) {
                std::filesystem::remove(folderIter.path());
            }
        }
    }

    static inline std::string appendWALFileSuffixIfNecessary(
        const std::string& fileName, common::DBFileType dbFileType) {
        return dbFileType == common::DBFileType::WAL_VERSION ? appendWALFileSuffix(fileName) :
                                                               fileName;
    }

    static inline std::string appendWALFileSuffix(const std::string& fileName) {
        assert(fileName.find(common::StorageConstants::WAL_FILE_SUFFIX) == std::string::npos);
        return fileName + common::StorageConstants::WAL_FILE_SUFFIX;
    }

    static std::unique_ptr<common::FileInfo> getFileInfoForReadWrite(
        const std::string& directory, StorageStructureID storageStructureID);

    static std::string getColumnFName(
        const std::string& directory, StorageStructureID storageStructureID);

    static std::string getListFName(
        const std::string& directory, StorageStructureID storageStructureID);

    static void initializeListsHeaders(common::table_id_t relTableID, uint64_t numNodesInTable,
        const std::string& directory, common::RelDataDirection relDirection);

    static uint32_t getDataTypeSize(const common::LogicalType& type);

private:
    static std::string appendSuffixOrInsertBeforeWALSuffix(
        std::string fileName, std::string suffix);
};

} // namespace storage
} // namespace kuzu
