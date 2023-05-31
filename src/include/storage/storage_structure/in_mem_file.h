#pragma once

#include <shared_mutex>

#include "arrow/array.h"
#include "common/constants.h"
#include "common/types/value.h"
#include "storage/storage_structure/in_mem_page.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

static const std::string IN_MEM_TEMP_FILE_PATH = "";

// InMemFile holds a collection of in-memory page in the memory.
class InMemFile {

public:
    InMemFile(
        std::string filePath, uint16_t numBytesForElement, bool hasNullMask, uint64_t numPages = 0);
    InMemFile(uint16_t numBytesForElement, bool hasNullMask, uint64_t numPages = 0)
        : InMemFile(IN_MEM_TEMP_FILE_PATH, numBytesForElement, hasNullMask, numPages) {}

    virtual ~InMemFile() = default;

    virtual void flush();

    void addNewPages(uint64_t numNewPagesToAdd, bool setToZero = false);

    uint32_t addANewPage(bool setToZero = false);

    inline InMemPage* getPage(common::page_idx_t pageIdx) const { return pages[pageIdx].get(); }

    uint64_t getNumPages() { return pages.size(); }

protected:
    std::string filePath;
    uint16_t numBytesForElement;
    uint64_t numElementsInAPage;
    bool hasNullMask;
    std::vector<std::unique_ptr<InMemPage>> pages;
};

//  InMemFile for storing overflow of PropertyColumn or PropertyLists.
class InMemOverflowFile : public InMemFile {

public:
    explicit InMemOverflowFile(const std::string& fName)
        : InMemFile{fName, 1 /* numBytesForElement */, false /* hasNullMask */, 1 /* numPages */},
          nextPageIdxToAppend{0}, nextOffsetInPageToAppend{0} {}
    explicit InMemOverflowFile()
        : InMemFile{1 /* numBytesForElement */, false /* hasNullMask */, 1 /* numPages */},
          nextPageIdxToAppend{0}, nextOffsetInPageToAppend{0} {}

    // NOTICE: appendString should not be called mixed with copyString/copyList. They have
    // in-compatible locking mechanisms.

    // This function appends a string if the length of the string is larger than SHORT_STR_LENGTH,
    // otherwise, construct the ku_string for the rawString and return it.
    // Multiple threads are coordinate by taking an exclusive lock each time it needs to copy
    // overflow values to the file and updating nextPageIdxToAppend/nextOffsetInPageToAppend.
    common::ku_string_t appendString(const char* rawString);
    // These two functions copies a string/list value to the file according to the cursor. Multiple
    // threads coordinate by that each thread takes the full control of a single page at a time.
    // When the page is not exhausted, each thread can write without an exclusive lock.
    common::ku_string_t copyString(
        const char* rawString, common::page_offset_t length, PageByteCursor& overflowCursor);
    common::ku_list_t copyList(const common::Value& listValue, PageByteCursor& overflowCursor);

    // Copy overflow data at srcOverflow into dstKUString.
    void copyStringOverflow(
        PageByteCursor& overflowCursor, uint8_t* srcOverflow, common::ku_string_t* dstKUString);
    void copyListOverflowFromFile(InMemOverflowFile* srcInMemOverflowFile,
        const PageByteCursor& srcOverflowCursor, PageByteCursor& dstOverflowCursor,
        common::ku_list_t* dstKUList, common::LogicalType* listChildDataType);
    void copyListOverflowToFile(PageByteCursor& pageByteCursor, common::ku_list_t* srcKUList,
        common::LogicalType* childDataType);

    std::string readString(common::ku_string_t* strInInMemOvfFile);

    common::ku_list_t appendList(common::LogicalType& type, arrow::ListArray& listArray,
        uint64_t pos, PageByteCursor& overflowCursor);

private:
    common::page_idx_t addANewOverflowPage();

    void copyFixedSizedValuesInList(const common::Value& listVal, PageByteCursor& overflowCursor,
        uint64_t numBytesOfListElement);
    template<common::LogicalTypeID DT>
    void copyVarSizedValuesInList(common::ku_list_t& resultKUList, const common::Value& listVal,
        PageByteCursor& overflowCursor, uint64_t numBytesOfListElement);

    void resetElementsOverflowPtrIfNecessary(PageByteCursor& pageByteCursor,
        common::LogicalType* elementType, uint64_t numElementsToReset, uint8_t* elementsToReset);

private:
    // These two fields (currentPageIdxToAppend, currentOffsetInPageToAppend) are used when
    // appendString to the file.
    common::page_idx_t nextPageIdxToAppend;
    common::page_offset_t nextOffsetInPageToAppend;
    std::shared_mutex lock;
};

} // namespace storage
} // namespace kuzu
