#pragma once

#include <string>

#include "wal/wal_record.h"

#include "src/catalog/include/catalog_structs.h"
#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/null_mask.h"
#include "src/common/include/utils.h"
#include "src/common/types/include/types_include.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

struct StorageStructureIDAndFName {
    StorageStructureIDAndFName(StorageStructureID storageStructureID, string fName)
        : storageStructureID{storageStructureID}, fName{move(fName)} {};

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
    PageElementCursor() : PageElementCursor{-1u, (uint16_t)-1} {};

    inline void nextPage() {
        pageIdx++;
        posInPage = 0;
    }

    page_idx_t pageIdx;
    uint16_t posInPage;
};

struct PageUtils {

    static uint32_t getNumElementsInAPage(uint32_t elementSize, bool hasNull) {
        auto numBytesPerNullEntry = NullMask::NUM_BITS_PER_NULL_ENTRY >> 3;
        auto numNullEntries =
            hasNull ? (uint32_t)ceil((double)DEFAULT_PAGE_SIZE /
                                     ((elementSize << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2) +
                                         numBytesPerNullEntry)) :
                      0;
        return (DEFAULT_PAGE_SIZE - (numNullEntries * numBytesPerNullEntry)) / elementSize;
    }

    // This function returns the page pageIdx of the page where element will be found and the pos of
    // the element in the page as the offset.
    static PageElementCursor getPageElementCursorForPos(
        const uint64_t& elementPos, const uint32_t numElementsPerPage) {
        assert((elementPos / numElementsPerPage) < UINT32_MAX);
        return PageElementCursor{(page_idx_t)(elementPos / numElementsPerPage),
            (uint16_t)(elementPos % numElementsPerPage)};
    }
};

class StorageUtils {

public:
    static void saveListOfIntsToFile(const string& fName, uint8_t* data, uint32_t listSize);
    static uint32_t readListOfIntsFromFile(unique_ptr<uint32_t[]>& data, const string& fName);

    inline static string getNodeIndexFName(const string& directory, const label_t& nodeLabel) {
        auto fName = StringUtils::string_format("n-%d", nodeLabel);
        return FileUtils::joinPath(directory, fName + StorageConfig::INDEX_FILE_SUFFIX);
    }
    inline static string getNodePropertyColumnFName(
        const string& directory, const label_t& nodeLabel, const string& propertyName) {
        auto fName = StringUtils::string_format("n-%d-%s", nodeLabel, propertyName.data());
        return FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX);
    }

    inline static StorageStructureIDAndFName getStructuredNodePropertyColumnStructureIDAndFName(
        const string& directory, const catalog::Property& property) {
        auto fName = getNodePropertyColumnFName(directory, property.label, property.name);
        return StorageStructureIDAndFName(StorageStructureID::newStructuredNodePropertyMainColumnID(
                                              property.label, property.propertyID),
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
        const label_t& nodeLabel, const RelDirection& relDirection, const string& propertyName) {
        auto fName = StringUtils::string_format(
            "r-%d-%d-%d-%s", relLabel, nodeLabel, relDirection, propertyName.data());
        return FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX);
    }

    inline static string getOverflowPagesFName(const string& fName) {
        return fName + StorageConfig::OVERFLOW_FILE_SUFFIX;
    }
};

} // namespace storage
} // namespace graphflow
