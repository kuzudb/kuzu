#pragma once

#include <cmath>
#include <string>

#include "common/constants.h"
#include "common/file_system/virtual_file_system.h"
#include "common/null_mask.h"
#include "common/system_config.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "main/settings.h"

namespace kuzu {
namespace storage {

struct PageCursor {
    PageCursor() : PageCursor{UINT32_MAX, UINT16_MAX} {};
    PageCursor(common::page_idx_t pageIdx, uint32_t posInPage)
        : pageIdx{pageIdx}, elemPosInPage{posInPage} {};

    void nextPage() {
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
                static_cast<uint32_t>(ceil(
                    static_cast<double>(common::KUZU_PAGE_SIZE) /
                    static_cast<double>((static_cast<uint64_t>(elementSize)
                                            << common::NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2) +
                                        numBytesPerNullEntry))) :
                0;
        return (common::KUZU_PAGE_SIZE - (numNullEntries * numBytesPerNullEntry)) / elementSize;
    }

    // This function returns the page pageIdx of the page where element will be found and the pos of
    // the element in the page as the offset.
    static PageCursor getPageCursorForPos(uint64_t elementPos, uint32_t numElementsPerPage) {
        KU_ASSERT((elementPos / numElementsPerPage) < UINT32_MAX);
        return PageCursor{static_cast<common::page_idx_t>(elementPos / numElementsPerPage),
            static_cast<uint32_t>(elementPos % numElementsPerPage)};
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
        return std::ceil(static_cast<double>(v1) / static_cast<double>(v2));
    }

    static std::string getColumnName(const std::string& propertyName, ColumnType type,
        const std::string& prefix);

    static common::offset_t getStartOffsetOfNodeGroup(common::node_group_idx_t nodeGroupIdx) {
        return nodeGroupIdx << common::StorageConfig::NODE_GROUP_SIZE_LOG2;
    }
    static common::node_group_idx_t getNodeGroupIdx(common::offset_t nodeOffset) {
        return nodeOffset >> common::StorageConfig::NODE_GROUP_SIZE_LOG2;
    }
    static std::pair<common::node_group_idx_t, common::offset_t> getNodeGroupIdxAndOffsetInChunk(
        common::offset_t nodeOffset) {
        auto nodeGroupIdx = getNodeGroupIdx(nodeOffset);
        auto offsetInChunk = nodeOffset - getStartOffsetOfNodeGroup(nodeGroupIdx);
        return std::make_pair(nodeGroupIdx, offsetInChunk);
    }

    static std::string getDataFName(common::VirtualFileSystem* vfs, const std::string& directory) {
        return vfs->joinPath(directory, common::StorageConstants::DATA_FILE_NAME);
    }

    static std::string getLockFilePath(common::VirtualFileSystem* vfs,
        const std::string& directory) {
        return vfs->joinPath(directory, common::StorageConstants::LOCK_FILE_NAME);
    }

    static std::string expandPath(main::ClientContext* context, const std::string& path) {
        if (main::DBConfig::isDBPathInMemory(path)) {
            return path;
        }
        auto fullPath = path;
        // Handle '~' for home directory expansion
        if (path.starts_with('~')) {
            fullPath = context->getCurrentSetting(main::HomeDirectorySetting::name)
                           .getValue<std::string>() +
                       fullPath.substr(1);
        }
        // Normalize the path to resolve '.' and '..'
        std::filesystem::path normalizedPath =
            std::filesystem::absolute(fullPath).lexically_normal();
        return normalizedPath.string();
    }

    // Note: This is a relatively slow function because of division and mod and making std::pair.
    // It is not meant to be used in performance critical code path.
    static std::pair<uint64_t, uint64_t> getQuotientRemainder(uint64_t i, uint64_t divisor) {
        return std::make_pair(i / divisor, i % divisor);
    }
    static uint64_t getQuotient(uint64_t i, uint64_t divisor) { return i / divisor; }

    static uint32_t getDataTypeSize(const common::LogicalType& type);
    static uint32_t getDataTypeSize(common::PhysicalTypeID type);
};

} // namespace storage
} // namespace kuzu
