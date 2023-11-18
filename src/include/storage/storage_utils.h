#pragma once

#include <cmath>
#include <string>

#include "common/constants.h"
#include "common/file_utils.h"
#include "common/null_mask.h"
#include "storage/wal/wal_record.h"

namespace kuzu {
namespace storage {

class StorageManager;

struct DBFileIDAndName {
    DBFileIDAndName(DBFileID dbFileID, std::string fName)
        : dbFileID{dbFileID}, fName{std::move(fName)} {};
    DBFileID dbFileID;
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
        KU_ASSERT(elementSize > 0);
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
        KU_ASSERT((elementPos / numElementsPerPage) < UINT32_MAX);
        return PageElementCursor{(common::page_idx_t)(elementPos / numElementsPerPage),
            (uint16_t)(elementPos % numElementsPerPage)};
    }

    static inline PageByteCursor getPageByteCursorForPos(
        uint64_t elementPos, uint32_t numElementsPerPage, uint64_t elementSize) {
        KU_ASSERT((elementPos / numElementsPerPage) < UINT32_MAX);
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
    static inline common::node_group_idx_t getNodeGroupIdx(common::offset_t nodeOffset) {
        return nodeOffset >> common::StorageConstants::NODE_GROUP_SIZE_LOG2;
    }
    static inline std::pair<common::node_group_idx_t, common::offset_t>
    getNodeGroupIdxAndOffsetInChunk(common::offset_t nodeOffset) {
        auto nodeGroupIdx = getNodeGroupIdx(nodeOffset);
        auto offsetInChunk = nodeOffset - getStartOffsetOfNodeGroup(nodeGroupIdx);
        return std::make_pair(nodeGroupIdx, offsetInChunk);
    }

    static std::string getNodeIndexFName(const std::string& directory,
        const common::table_id_t& tableID, common::FileVersionType dbFileType);

    static inline std::string getDataFName(const std::string& directory) {
        return common::FileUtils::joinPath(directory, common::StorageConstants::DATA_FILE_NAME);
    }

    static inline std::string getMetadataFName(const std::string& directory) {
        return common::FileUtils::joinPath(directory, common::StorageConstants::METADATA_FILE_NAME);
    }

    static inline DBFileIDAndName getNodeIndexIDAndFName(
        const std::string& directory, common::table_id_t tableID) {
        auto fName = getNodeIndexFName(directory, tableID, common::FileVersionType::ORIGINAL);
        return {DBFileID::newPKIndexFileID(tableID), fName};
    }

    static inline std::string getOverflowFileName(const std::string& fName) {
        return appendSuffixOrInsertBeforeWALSuffix(
            fName, common::StorageConstants::OVERFLOW_FILE_SUFFIX);
    }

    static inline void overwriteNodesStatisticsAndDeletedIDsFileWithVersionFromWAL(
        const std::string& directory) {
        common::FileUtils::overwriteFile(getNodesStatisticsAndDeletedIDsFilePath(
                                             directory, common::FileVersionType::WAL_VERSION),
            getNodesStatisticsAndDeletedIDsFilePath(directory, common::FileVersionType::ORIGINAL));
    }

    static inline std::string getNodesStatisticsAndDeletedIDsFilePath(
        const std::string& directory, common::FileVersionType dbFileType) {
        return common::FileUtils::joinPath(
            directory, dbFileType == common::FileVersionType::ORIGINAL ?
                           common::StorageConstants::NODES_STATISTICS_AND_DELETED_IDS_FILE_NAME :
                           common::StorageConstants::NODES_STATISTICS_FILE_NAME_FOR_WAL);
    }

    static inline void overwriteRelsStatisticsFileWithVersionFromWAL(const std::string& directory) {
        common::FileUtils::overwriteFile(
            getRelsStatisticsFilePath(directory, common::FileVersionType::WAL_VERSION),
            getRelsStatisticsFilePath(directory, common::FileVersionType::ORIGINAL));
    }

    static inline std::string getRelsStatisticsFilePath(
        const std::string& directory, common::FileVersionType dbFileType) {
        return common::FileUtils::joinPath(
            directory, dbFileType == common::FileVersionType::ORIGINAL ?
                           common::StorageConstants::RELS_METADATA_FILE_NAME :
                           common::StorageConstants::RELS_METADATA_FILE_NAME_FOR_WAL);
    }

    static inline void overwriteCatalogFileWithVersionFromWAL(const std::string& directory) {
        common::FileUtils::overwriteFile(
            getCatalogFilePath(directory, common::FileVersionType::WAL_VERSION),
            getCatalogFilePath(directory, common::FileVersionType::ORIGINAL));
    }

    static inline std::string getCatalogFilePath(
        const std::string& directory, common::FileVersionType dbFileType) {
        return common::FileUtils::joinPath(
            directory, dbFileType == common::FileVersionType::ORIGINAL ?
                           common::StorageConstants::CATALOG_FILE_NAME :
                           common::StorageConstants::CATALOG_FILE_NAME_FOR_WAL);
    }

    static inline std::string getLockFilePath(const std::string& directory) {
        return common::FileUtils::joinPath(directory, common::StorageConstants::LOCK_FILE_NAME);
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
        const std::string& fileName, common::FileVersionType fileVersionType) {
        return fileVersionType == common::FileVersionType::WAL_VERSION ?
                   appendWALFileSuffix(fileName) :
                   fileName;
    }

    static inline std::string appendWALFileSuffix(const std::string& fileName) {
        KU_ASSERT(fileName.find(common::StorageConstants::WAL_FILE_SUFFIX) == std::string::npos);
        return fileName + common::StorageConstants::WAL_FILE_SUFFIX;
    }

    static std::unique_ptr<common::FileInfo> getFileInfoForReadWrite(
        const std::string& directory, DBFileID dbFileID);

    static uint32_t getDataTypeSize(const common::LogicalType& type);

private:
    static std::string appendSuffixOrInsertBeforeWALSuffix(
        const std::string& fileName, const std::string& suffix);
};

} // namespace storage
} // namespace kuzu
