#pragma once

#include <cmath>
#include <string>

#include "common/constants.h"
#include "common/file_system/virtual_file_system.h"
#include "common/null_mask.h"
#include "common/types/types.h"
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

struct PageCursor {
    PageCursor(common::page_idx_t pageIdx, uint16_t posInPage)
        : pageIdx{pageIdx}, elemPosInPage{posInPage} {};
    PageCursor() : PageCursor{UINT32_MAX, UINT16_MAX} {};

    inline void nextPage() {
        pageIdx++;
        elemPosInPage = 0;
    }

    common::page_idx_t pageIdx;
    // Larger than necessary, but PageCursor is directly written to disk
    // and adding an explicit padding field messes with structured bindings
    uint32_t elemPosInPage;
};
static_assert(std::has_unique_object_representations_v<PageCursor>);

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
    static inline PageCursor getPageCursorForPos(uint64_t elementPos, uint32_t numElementsPerPage) {
        KU_ASSERT((elementPos / numElementsPerPage) < UINT32_MAX);
        return PageCursor{(common::page_idx_t)(elementPos / numElementsPerPage),
            (uint16_t)(elementPos % numElementsPerPage)};
    }
};

class StorageUtils {
public:
    enum class ColumnType {
        DEFAULT = 0,
        INDEX = 1,  // This is used for index columns in STRING columns.
        OFFSET = 2, // This is used for offset columns in LIST and STRING columns.
        DATA = 3,   // This is used for data columns in LIST and STRING columns.
        CSR_OFFSET = 4,
        CSR_LENGTH = 5,
        STRUCT_CHILD = 6,
        NULL_MASK = 7,
    };

    // TODO: Constrain T1 and T2 to numerics.
    template<typename T1, typename T2>
    static uint64_t divideAndRoundUpTo(T1 v1, T2 v2) {
        return std::ceil((double)v1 / (double)v2);
    }

    static std::string getColumnName(const std::string& propertyName, ColumnType type,
        const std::string& prefix);

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

    static std::string getNodeIndexFName(const common::VirtualFileSystem* vfs,
        const std::string& directory, const common::table_id_t& tableID,
        common::FileVersionType dbFileType);

    static inline std::string getDataFName(common::VirtualFileSystem* vfs,
        const std::string& directory) {
        return vfs->joinPath(directory, common::StorageConstants::DATA_FILE_NAME);
    }

    static inline std::string getMetadataFName(common::VirtualFileSystem* vfs,
        const std::string& directory) {
        return vfs->joinPath(directory, common::StorageConstants::METADATA_FILE_NAME);
    }

    static inline DBFileIDAndName getNodeIndexIDAndFName(common::VirtualFileSystem* vfs,
        const std::string& directory, common::table_id_t tableID) {
        auto fName = getNodeIndexFName(vfs, directory, tableID, common::FileVersionType::ORIGINAL);
        return {DBFileID::newPKIndexFileID(tableID), fName};
    }

    static inline std::string getOverflowFileName(const std::string& fName) {
        return appendSuffixOrInsertBeforeWALSuffix(fName,
            common::StorageConstants::OVERFLOW_FILE_SUFFIX);
    }

    static inline std::string getNodesStatisticsAndDeletedIDsFilePath(
        common::VirtualFileSystem* vfs, const std::string& directory,
        common::FileVersionType dbFileType) {
        return vfs->joinPath(directory,
            dbFileType == common::FileVersionType::ORIGINAL ?
                common::StorageConstants::NODES_STATISTICS_AND_DELETED_IDS_FILE_NAME :
                common::StorageConstants::NODES_STATISTICS_FILE_NAME_FOR_WAL);
    }

    static inline std::string getRelsStatisticsFilePath(common::VirtualFileSystem* vfs,
        const std::string& directory, common::FileVersionType dbFileType) {
        return vfs->joinPath(directory,
            dbFileType == common::FileVersionType::ORIGINAL ?
                common::StorageConstants::RELS_METADATA_FILE_NAME :
                common::StorageConstants::RELS_METADATA_FILE_NAME_FOR_WAL);
    }

    static inline std::string getCatalogFilePath(common::VirtualFileSystem* vfs,
        const std::string& directory, common::FileVersionType dbFileType) {
        return vfs->joinPath(directory, dbFileType == common::FileVersionType::ORIGINAL ?
                                            common::StorageConstants::CATALOG_FILE_NAME :
                                            common::StorageConstants::CATALOG_FILE_NAME_FOR_WAL);
    }

    static inline std::string getLockFilePath(common::VirtualFileSystem* vfs,
        const std::string& directory) {
        return vfs->joinPath(directory, common::StorageConstants::LOCK_FILE_NAME);
    }

    // Note: This is a relatively slow function because of division and mod and making std::pair.
    // It is not meant to be used in performance critical code path.
    static inline std::pair<uint64_t, uint64_t> getQuotientRemainder(uint64_t i, uint64_t divisor) {
        return std::make_pair(i / divisor, i % divisor);
    }

    static inline void removeCatalogAndStatsWALFiles(const std::string& directory,
        common::VirtualFileSystem* vfs) {
        vfs->removeFileIfExists(
            StorageUtils::getCatalogFilePath(vfs, directory, common::FileVersionType::WAL_VERSION));
        vfs->removeFileIfExists(StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(vfs,
            directory, common::FileVersionType::WAL_VERSION));
        vfs->removeFileIfExists(StorageUtils::getRelsStatisticsFilePath(vfs, directory,
            common::FileVersionType::WAL_VERSION));
    }

    static inline std::string appendWALFileSuffixIfNecessary(const std::string& fileName,
        common::FileVersionType fileVersionType) {
        return fileVersionType == common::FileVersionType::WAL_VERSION ?
                   appendWALFileSuffix(fileName) :
                   fileName;
    }

    static inline std::string appendWALFileSuffix(const std::string& fileName) {
        KU_ASSERT(fileName.find(common::StorageConstants::WAL_FILE_SUFFIX) == std::string::npos);
        return fileName + common::StorageConstants::WAL_FILE_SUFFIX;
    }

    static std::unique_ptr<common::FileInfo> getFileInfoForReadWrite(const std::string& directory,
        DBFileID dbFileID, common::VirtualFileSystem* vfs);

    static uint32_t getDataTypeSize(const common::LogicalType& type);
    static uint32_t getDataTypeSize(common::PhysicalTypeID type);

private:
    static std::string appendSuffixOrInsertBeforeWALSuffix(const std::string& fileName,
        const std::string& suffix);
};

} // namespace storage
} // namespace kuzu
