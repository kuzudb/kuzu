#pragma once

#include <shared_mutex>

#include "src/common/include/configs.h"
#include "src/common/types/include/literal.h"
#include "src/loader/include/in_mem_structure/in_mem_page.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/storage_utils.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

// InMemFile holds a collection of in-memory page in the memory.
class InMemFile {

public:
    InMemFile(
        std::string filePath, uint16_t numBytesForElement, bool hasNullMask, uint64_t numPages);
    InMemFile(uint16_t numBytesForElement, bool hasNullMask, uint64_t numPages)
        : InMemFile("", numBytesForElement, hasNullMask, numPages) {}

    virtual ~InMemFile() = default;

    virtual void flush();

protected:
    uint32_t addANewPage();

public:
    string filePath;
    uint64_t numElementsInAPage;
    uint32_t numUsedPages;
    bool hasNullMask;
    vector<unique_ptr<InMemPage>> pages;
};

//  InMemFile for storing overflow of PropertyColumn or PropertyLists.
class InMemOverflowFile : public InMemFile {

public:
    explicit InMemOverflowFile(const std::string& fName) : InMemFile{fName, 1, false, 8} {}
    explicit InMemOverflowFile() : InMemFile{1, false, 8} {}

    gf_string_t addString(const char* rawString, PageByteCursor& overflowCursor);
    gf_list_t addList(const Literal& listLiteral, PageByteCursor& overflowCursor);
    // Copy overflow data at srcOverflow into dstGFString.
    void copyStringOverflow(
        PageByteCursor& overflowCursor, uint8_t* srcOverflow, gf_string_t* dstGFString);
    void copyListOverflow(InMemOverflowFile* srcOverflowPages,
        const PageByteCursor& srcOverflowCursor, PageByteCursor& dstOverflowCursor,
        gf_list_t* dstGFList, DataType* listChildDataType);

private:
    uint32_t getNewOverflowPageIdx();

    void copyFixedSizedValuesToPages(
        const Literal& listVal, PageByteCursor& overflowCursor, uint64_t numBytesOfListElement);
    template<DataTypeID DT>
    void copyVarSizedValuesToPages(gf_list_t& resultGFList, const Literal& listVal,
        PageByteCursor& overflowCursor, uint64_t numBytesOfListElement);

private:
    std::shared_mutex lock;
};

} // namespace loader
} // namespace graphflow
