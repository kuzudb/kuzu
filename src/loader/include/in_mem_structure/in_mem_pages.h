#pragma once

#include <memory>
#include <shared_mutex>
#include <utility>

#include "src/common/include/configs.h"
#include "src/common/include/gf_string.h"
#include "src/common/include/types.h"
#include "src/loader/include/in_mem_structure/in_mem_page.h"
#include "src/storage/include/storage_structure/storage_structure.h"

using namespace graphflow::storage;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

// InMemPages holds a collection of in-memory page in the memory.
class InMemPages {

public:
    InMemPages(string fName, uint16_t numBytesForElement, bool hasNULLBytes, uint64_t numPages);

    virtual void saveToFile();

protected:
    const string fName;
    unique_ptr<uint8_t[]> data;
    vector<unique_ptr<InMemPage>> pages;
    uint16_t numBytesForElement;
};

// InMemPages for storing AdjColumns or AdjLists.
class InMemAdjPages : public InMemPages {

public:
    InMemAdjPages(const string& fName, uint8_t numBytesPerLabel, uint8_t numBytesPerOffset,
        uint64_t numPages, bool hasNULLBytes)
        : InMemPages{fName, uint16_t(numBytesPerLabel + numBytesPerOffset), hasNULLBytes, numPages},
          numBytesPerLabel{numBytesPerLabel}, numBytesPerOffset{numBytesPerOffset} {};

    void set(const PageElementCursor& cursor, const nodeID_t& nbrNodeID);

protected:
    const uint8_t numBytesPerLabel;
    const uint8_t numBytesPerOffset;
};

// InMemPages for storing PropertyColumn or PropertyLists.
class InMemPropertyPages : public InMemPages {

public:
    InMemPropertyPages(const string& fName, uint16_t numBytesPerElement, uint64_t numPages)
        : InMemPages{fName, numBytesPerElement, true, numPages},
          numBytesPerElement(numBytesPerElement){};

    uint8_t* getPtrToMemLoc(const PageElementCursor& cursor) {
        return pages[cursor.idx]->getPtrToMemLoc(cursor.pos * numBytesForElement);
    }

    void set(const PageElementCursor& cursor, const uint8_t* val);

protected:
    uint32_t numBytesPerElement;
};

// InMemPages for storing Nodes' unstructured PropertyLists.
class InMemUnstrPropertyPages : public InMemPages {

public:
    InMemUnstrPropertyPages(const string& fName, uint64_t numPages)
        : InMemPages(fName, 1 /*element size*/, false, numPages) {}

    void set(const PageByteCursor& cursor, uint32_t propertyKey, uint8_t dataTypeIdentifier,
        uint32_t valLen, const uint8_t* val);

private:
    uint8_t* getPtrToMemLoc(const PageByteCursor& cursor) {
        return pages[cursor.idx]->getPtrToMemLoc(cursor.offset);
    }

    void setComponentOfUnstrProperty(PageByteCursor& localCursor, uint8_t len, const uint8_t* val);
};

//  InMemPages for storing string overflow of PropertyColumn or PropertyLists.
class InMemStringOverflowPages : public InMemPages {

public:
    explicit InMemStringOverflowPages(const string& fName) : InMemPages(fName, 1, false, 8) {
        numUsedPages = 0;
    };

    InMemStringOverflowPages() : InMemStringOverflowPages(""){};

    uint8_t* getPtrToMemLoc(const PageByteCursor& cursor) {
        return pages[cursor.idx]->getPtrToMemLoc(cursor.offset);
    }

    void setStrInOvfPageAndPtrInEncString(
        const char* originalString, PageByteCursor& cursor, gf_string_t* encodedString);

    void copyOverflowString(PageByteCursor& cursor, uint8_t* ptrToCopy, gf_string_t* encodedString);

    void saveToFile() override;

private:
    uint32_t getNewOverflowPageIdx();

private:
    shared_mutex lock;
    uint64_t numUsedPages;
};

} // namespace loader
} // namespace graphflow
