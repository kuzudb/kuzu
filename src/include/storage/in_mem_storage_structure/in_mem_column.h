#pragma once

#include "storage/node_id_compression_scheme.h"
#include "storage/storage_structure/in_mem_file.h"

namespace kuzu {
namespace storage {

class InMemColumn {

public:
    // For structured property columns.
    InMemColumn(string fName, DataType dataType, uint64_t numBytesForElement, uint64_t numElements);

    virtual ~InMemColumn() = default;

    virtual void saveToFile();

    virtual void setElement(node_offset_t offset, const uint8_t* val);
    inline uint8_t* getElement(node_offset_t offset) {
        auto cursor = getPageElementCursorForOffset(offset);
        return inMemFile->getPage(cursor.pageIdx)->data +
               (cursor.elemPosInPage * numBytesForElement);
    }

    virtual inline InMemOverflowFile* getInMemOverflowFile() { return nullptr; }

    inline DataType getDataType() { return dataType; }

    inline bool isNullAtNodeOffset(node_offset_t nodeOffset) {
        auto cursor = getPageElementCursorForOffset(nodeOffset);
        return inMemFile->getPage(cursor.pageIdx)->isElemPosNull(cursor.elemPosInPage);
    }

protected:
    inline PageElementCursor getPageElementCursorForOffset(node_offset_t offset) const {
        return PageElementCursor{
            (page_idx_t)(offset / numElementsInAPage), (uint16_t)(offset % numElementsInAPage)};
    }

protected:
    string fName;
    DataType dataType;
    uint64_t numBytesForElement;
    uint64_t numElementsInAPage;
    unique_ptr<InMemFile> inMemFile;
};

class InMemColumnWithOverflow : public InMemColumn {

protected:
    InMemColumnWithOverflow(string fName, DataType dataType, uint64_t numElements);

    ~InMemColumnWithOverflow() override = default;

    void saveToFile() override;

    inline InMemOverflowFile* getInMemOverflowFile() override { return inMemOverflowFile.get(); }

protected:
    unique_ptr<InMemOverflowFile> inMemOverflowFile;
};

class InMemAdjColumn : public InMemColumn {

public:
    InMemAdjColumn(
        string fName, const NodeIDCompressionScheme& nodeIDCompressionScheme, uint64_t numElements)
        : InMemColumn{move(fName), DataType(NODE_ID),
              nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression(), numElements},
          nodeIDCompressionScheme{nodeIDCompressionScheme} {};

    ~InMemAdjColumn() override = default;

    void setElement(node_offset_t offset, const uint8_t* val) override;

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
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

} // namespace storage
} // namespace kuzu
