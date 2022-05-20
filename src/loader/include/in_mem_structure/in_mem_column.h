#pragma once

#include "src/loader/include/in_mem_structure/in_mem_pages.h"
#include "src/storage/include/compression_scheme.h"
#include "src/storage/include/storage_structure/overflow_pages.h"

namespace graphflow {
namespace loader {

class InMemColumn {

public:
    // For structured property columns.
    InMemColumn(string fName, DataType dataType, uint64_t numBytesForElement, uint64_t numElements);

    virtual ~InMemColumn() = default;

    virtual void saveToFile();

    virtual void setElement(node_offset_t offset, const uint8_t* val);
    inline uint8_t* getElement(node_offset_t offset) {
        auto cursor = getPageElementCursorForOffset(offset);
        return pages->pages[cursor.pageIdx]->data + (cursor.pos * numBytesForElement);
    }

    virtual inline InMemOverflowPages* getOverflowPages() { return nullptr; }

    inline DataType getDataType() { return dataType; }

protected:
    inline PageElementCursor getPageElementCursorForOffset(node_offset_t offset) const {
        return PageElementCursor{
            offset / numElementsInAPage, (uint16_t)(offset % numElementsInAPage)};
    }

protected:
    string fName;
    DataType dataType;
    uint64_t numBytesForElement;
    uint64_t numElementsInAPage;
    unique_ptr<InMemPages> pages;
};

class InMemColumnWithOverflow : public InMemColumn {

protected:
    InMemColumnWithOverflow(string fName, DataType dataType, uint64_t numElements);

    ~InMemColumnWithOverflow() override = default;

    void saveToFile() override;

    inline InMemOverflowPages* getOverflowPages() override { return overflowPages.get(); }

protected:
    unique_ptr<InMemOverflowPages> overflowPages;
};

class InMemAdjColumn : public InMemColumn {

public:
    InMemAdjColumn(
        string fName, const NodeIDCompressionScheme& compressionScheme, uint64_t numElements)
        : InMemColumn{move(fName), DataType(NODE_ID), compressionScheme.getNumTotalBytes(),
              numElements},
          compressionScheme{compressionScheme} {};

    ~InMemAdjColumn() override = default;

    void setElement(node_offset_t offset, const uint8_t* val) override;

private:
    NodeIDCompressionScheme compressionScheme;
};

class InMemStringColumn : public InMemColumnWithOverflow {

public:
    InMemStringColumn(string fName, uint64_t numElements)
        : InMemColumnWithOverflow{move(fName), DataType(STRING), numElements} {}
};

class InMemListColumn : public InMemColumnWithOverflow {

public:
    InMemListColumn(string fName, DataType dataType, uint64_t numElements)
        : InMemColumnWithOverflow{move(fName), move(dataType), numElements} {
        assert(this->dataType.typeID == LIST);
    };
};

class InMemColumnFactory {

public:
    static unique_ptr<InMemColumn> getInMemPropertyColumn(
        const string& fName, const DataType& dataType, uint64_t numElements);
};

} // namespace loader
} // namespace graphflow
