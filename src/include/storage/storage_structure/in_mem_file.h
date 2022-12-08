#pragma once

#include <shared_mutex>

#include "common/configs.h"
#include "common/types/literal.h"
#include "storage/storage_structure/in_mem_page.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace storage {

static const string IN_MEM_TEMP_FILE_PATH = "";

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

    inline InMemPage* getPage(page_idx_t pageIdx) const { return pages[pageIdx].get(); }

    uint64_t getNumPages() { return pages.size(); }

protected:
    string filePath;
    uint16_t numBytesForElement;
    uint64_t numElementsInAPage;
    bool hasNullMask;
    vector<unique_ptr<InMemPage>> pages;
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

    // This function appends a string if the length of the string
    // is larger than SHORT_STR_LENGTH, otherwise, construct the ku_string for the rawString and
    // return it. Multiple threads coordinate by taking an exclusive lock each time it needs to copy
    // overflow values to the file and updating nextPageIdxToAppend/nextOffsetInPageToAppend.
    ku_string_t appendString(const char* rawString);
    // These two functions copies a string/list value to the file according to the cursor. Multiple
    // threads coordinate by that each thread takes the full control of a single page at a time.
    // When the page is not exhausted, each thread can write without an exclusive lock.
    ku_string_t copyString(const char* rawString, PageByteCursor& overflowCursor);
    ku_list_t copyList(const Literal& listLiteral, PageByteCursor& overflowCursor);

    // Copy overflow data at srcOverflow into dstKUString.
    void copyStringOverflow(
        PageByteCursor& overflowCursor, uint8_t* srcOverflow, ku_string_t* dstKUString);
    void copyListOverflow(InMemOverflowFile* srcInMemOverflowFile,
        const PageByteCursor& srcOverflowCursor, PageByteCursor& dstOverflowCursor,
        ku_list_t* dstKUList, DataType* listChildDataType);

    string readString(ku_string_t* strInInMemOvfFile);

private:
    uint32_t addANewOverflowPage();

    void copyFixedSizedValuesToPages(
        const Literal& listVal, PageByteCursor& overflowCursor, uint64_t numBytesOfListElement);
    template<DataTypeID DT>
    void copyVarSizedValuesToPages(ku_list_t& resultKUList, const Literal& listVal,
        PageByteCursor& overflowCursor, uint64_t numBytesOfListElement);

private:
    // These two fields (currentPageIdxToAppend, currentOffsetInPageToAppend) are used when
    // appendString to the file.
    page_idx_t nextPageIdxToAppend;
    uint32_t nextOffsetInPageToAppend;
    std::shared_mutex lock;
};

} // namespace storage
} // namespace kuzu
