#pragma once

#include <shared_mutex>

#include "src/common/include/configs.h"
#include "src/loader/include/in_mem_structure/in_mem_page.h"
#include "src/storage/include/storage_utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace loader {

// InMemPages holds a collection of in-memory page in the memory.
class InMemPages {

public:
    InMemPages(
        std::string fName, uint16_t numBytesForElement, bool hasNULLBytes, uint64_t numPages);

    virtual ~InMemPages() = default;

    virtual void saveToFile();

public:
    const std::string fName;
    std::unique_ptr<uint8_t[]> data;
    vector<std::unique_ptr<InMemPage>> pages;
};

//  InMemPages for storing overflow of PropertyColumn or PropertyLists.
class InMemOverflowPages : public InMemPages {

public:
    explicit InMemOverflowPages(const std::string& fName) : InMemPages(fName, 1, false, 8) {
        numUsedPages = 0;
    };

    InMemOverflowPages() : InMemOverflowPages(""){};

    ~InMemOverflowPages() override = default;

    gf_string_t addString(const char* rawString, PageByteCursor& overflowCursor);
    gf_list_t addList(const Literal& listLiteral, PageByteCursor& overflowCursor);
    // Copy overflow data at srcOverflow into dstGFString.
    void copyStringOverflow(
        PageByteCursor& overflowCursor, uint8_t* srcOverflow, gf_string_t* dstGFString);
    void copyListOverflow(InMemOverflowPages* srcOverflowPages,
        const PageByteCursor& srcOverflowCursor, PageByteCursor& dstOverflowCursor,
        gf_list_t* dstGFList, DataType* listChildDataType);

    void saveToFile() override;

private:
    uint32_t getNewOverflowPageIdx();

    void copyFixedSizedValuesToPages(
        const Literal& listVal, PageByteCursor& overflowCursor, uint64_t numBytesOfListElement);
    template<DataTypeID DT>
    void copyVarSizedValuesToPages(gf_list_t& resultGFList, const Literal& listVal,
        PageByteCursor& overflowCursor, uint64_t numBytesOfListElement);

private:
    std::shared_mutex lock;
    uint64_t numUsedPages;
};

} // namespace loader
} // namespace graphflow
