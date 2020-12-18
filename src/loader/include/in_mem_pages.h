#pragma once

#include <iostream>
#include <memory>
#include <mutex>

#include "src/common/include/configs.h"
#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace loader {

//! InMemPages holds a collection of pages in memory for storing a column or lists.
class InMemPages {

public:
    InMemPages(const string& fname, const uint64_t& numPages) : fname{fname}, numPages{numPages} {
        auto size = numPages * PAGE_SIZE;
        data = make_unique<uint8_t[]>(size);
        fill(data.get(), data.get() + size, UINT8_MAX);
    };

    void saveToFile();

protected:
    unique_ptr<uint8_t[]> data;
    const string fname;
    uint64_t numPages;
};

//! Holds the cursor to reference a location in an InMemPages.
typedef struct PageCursor {
    uint64_t idx = -1;
    uint16_t offset = -1;
} PageCursor;

//! InMemPages for storing AdjRels or AdjLists.
class InMemAdjPages : public InMemPages {

public:
    InMemAdjPages(const string& fname, const uint64_t& numPages, const uint8_t& numBytesPerLabel,
        const uint8_t& numBytesPerOffset)
        : InMemPages{fname, numPages}, numBytesPerLabel{numBytesPerLabel},
          numBytesPerOffset(numBytesPerOffset){};

    void set(const PageCursor& cursor, const nodeID_t& nbrNodeID);

protected:
    const uint8_t numBytesPerLabel;
    const uint8_t numBytesPerOffset;
};

//! InMemPages for storing PropertyColumn or PropertyLists.
class InMemPropertyPages : public InMemPages {

public:
    InMemPropertyPages(const string& fname, uint64_t numPages, const uint8_t& numBytesPerElement)
        : InMemPages{fname, numPages}, numBytesPerElement{numBytesPerElement} {};

    inline void set(const PageCursor& cursor, const uint8_t* val) {
        memcpy(get(cursor), val, numBytesPerElement);
    };

    inline uint8_t* get(const PageCursor& cursor) {
        return data.get() + (PAGE_SIZE * cursor.idx) + cursor.offset;
    }

private:
    const uint8_t numBytesPerElement;
};

//! InMemPages for storing string overflow of PropertyColumn or PropertyLists.
class InMemStringOverflowPages : public InMemPages {

public:
    InMemStringOverflowPages(const string& fname) : InMemPages{fname, 8} {
        maxPages = numPages;
        numPages = 0;
    };

    InMemStringOverflowPages() : InMemPages{"", 8} {
        maxPages = numPages;
        numPages = 0;
    };

    void set(const char* originalString, PageCursor& cursor, gf_string_t* encodedString);

    void copyOverflowString(PageCursor& cursor, uint8_t* ptrToCopy, gf_string_t* encodedString);

    uint8_t* get(PageCursor& cursor) {
        return data.get() + (PAGE_SIZE * cursor.idx) + cursor.offset;
    }

private:
    uint32_t getNewOverflowPageIdx();

private:
    mutex lock;
    uint64_t maxPages;
};

} // namespace loader
} // namespace graphflow
