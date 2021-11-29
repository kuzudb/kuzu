#pragma once

#include <iostream>
#include <memory>
#include <shared_mutex>

#include "src/common/include/configs.h"
#include "src/common/include/gf_string.h"
#include "src/common/include/types.h"
#include "src/storage/include/data_structure/data_structure.h"

using namespace graphflow::storage;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

// InMemPages holds a collection of pages in memory for storing a column or lists.
class InMemPages {

public:
    InMemPages(string fName, const uint64_t& numPages) : fName{move(fName)}, numPages{numPages} {
        auto size = numPages << PAGE_SIZE_LOG_2;
        data = make_unique<uint8_t[]>(size);
        fill(data.get(), data.get() + size, UINT8_MAX);
    };

    void saveToFile();

protected:
    unique_ptr<uint8_t[]> data;
    const string fName;
    uint64_t numPages;
};

// InMemPages for storing AdjColumns or AdjLists.
class InMemAdjPages : public InMemPages {

public:
    InMemAdjPages(const string& fName, const uint64_t& numPages, const uint8_t& numBytesPerLabel,
        const uint8_t& numBytesPerOffset)
        : InMemPages{fName, numPages}, numBytesPerLabel{numBytesPerLabel},
          numBytesPerOffset(numBytesPerOffset){};

    void setNbrNode(const PageCursor& cursor, const nodeID_t& nbrNodeID);

protected:
    const uint8_t numBytesPerLabel;
    const uint8_t numBytesPerOffset;
};

// InMemPages for storing PropertyColumn or PropertyLists.
class InMemPropertyPages : public InMemPages {

public:
    InMemPropertyPages(const string& fName, uint64_t numPages, const uint8_t& numBytesPerElement)
        : InMemPages{fName, numPages}, numBytesPerElement{numBytesPerElement} {};

    inline void setPorperty(const PageCursor& cursor, const uint8_t* val) {
        memcpy(getPtrToMemLoc(cursor), val, numBytesPerElement);
    };

    inline uint8_t* getPtrToMemLoc(const PageCursor& cursor) {
        return data.get() + (cursor.idx << PAGE_SIZE_LOG_2) + cursor.offset;
    }

private:
    const uint8_t numBytesPerElement;
};

// InMemPages for storing Nodes' unstructured PropertyLists.
class InMemUnstrPropertyPages : public InMemPages {

public:
    InMemUnstrPropertyPages(const string& fName, uint64_t numPages)
        : InMemPages{fName, numPages} {};

    inline uint8_t* getPtrToMemLoc(const PageCursor& cursor) {
        return data.get() + (cursor.idx << PAGE_SIZE_LOG_2) + cursor.offset;
    }

    void setUnstrProperty(const PageCursor& cursor, uint32_t propertyKey,
        uint8_t dataTypeIdentifier, uint32_t valLen, const uint8_t* val);
};

//  InMemPages for storing string overflow of PropertyColumn or PropertyLists.
class InMemStringOverflowPages : public InMemPages {

public:
    explicit InMemStringOverflowPages(const string& fName) : InMemPages{fName, 8} {
        maxPages = numPages;
        numPages = 0;
    };

    InMemStringOverflowPages() : InMemPages{"", 8} {
        maxPages = numPages;
        numPages = 0;
    };

    void setStrInOvfPageAndPtrInEncString(
        const char* originalString, PageCursor& cursor, gf_string_t* encodedString);

    void copyOverflowString(PageCursor& cursor, uint8_t* ptrToCopy, gf_string_t* encodedString);

    uint8_t* getPtrToMemLoc(PageCursor& cursor) {
        return data.get() + (cursor.idx << PAGE_SIZE_LOG_2) + cursor.offset;
    }

private:
    uint32_t getNewOverflowPageIdx();

private:
    shared_mutex lock;
    uint64_t maxPages;
};

} // namespace loader
} // namespace graphflow
