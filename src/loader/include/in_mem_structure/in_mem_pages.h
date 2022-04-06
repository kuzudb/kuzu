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

    gf_string_t addString(const char* originalString, PageByteCursor& cursor);
    gf_list_t addList(const Literal& listVal, PageByteCursor& cursor);
    void copyStringOverflow(PageByteCursor& cursor, uint8_t* ptrToCopy, gf_string_t* encodedString);
    void copyListOverflow(PageByteCursor& cursor, uint8_t* ptrToCopy,
        InMemOverflowPages* overflowPagesToCopy, gf_list_t* encodedList, DataType* childDataType);

    void saveToFile() override;

private:
    uint32_t getNewOverflowPageIdx();

    template<DataTypeID DT>
    void copyOverflowValuesToPages(gf_list_t& result, const Literal& listVal,
        PageByteCursor& cursor, uint64_t numBytesOfSingleValue);

private:
    std::shared_mutex lock;
    uint64_t numUsedPages;
};

} // namespace loader
} // namespace graphflow
