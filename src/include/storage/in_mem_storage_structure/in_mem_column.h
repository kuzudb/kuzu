#pragma once

#include <functional>

#include "storage/node_id_compression_scheme.h"
#include "storage/storage_structure/in_mem_file.h"

namespace kuzu {
namespace storage {

class InMemColumn;

using fill_in_mem_column_function_t =
    std::function<void(InMemColumn* inMemColumn, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, offset_t nodeOffset, const DataType& dataType)>;

class InMemColumn {

public:
    // For property columns.
    InMemColumn(string fName, DataType dataType, uint64_t numBytesForElement, uint64_t numElements);

    void fillWithDefaultVal(uint8_t* defaultVal, uint64_t numNodes, const DataType& dataType);

    virtual ~InMemColumn() = default;

    virtual void saveToFile();

    virtual void setElement(offset_t offset, const uint8_t* val);
    inline uint8_t* getElement(offset_t offset) {
        auto cursor = getPageElementCursorForOffset(offset);
        return inMemFile->getPage(cursor.pageIdx)->data +
               (cursor.elemPosInPage * numBytesForElement);
    }

    virtual inline InMemOverflowFile* getInMemOverflowFile() { return nullptr; }

    inline DataType getDataType() { return dataType; }

    inline bool isNullAtNodeOffset(offset_t nodeOffset) {
        auto cursor = getPageElementCursorForOffset(nodeOffset);
        return inMemFile->getPage(cursor.pageIdx)->isElemPosNull(cursor.elemPosInPage);
    }

protected:
    inline PageElementCursor getPageElementCursorForOffset(offset_t offset) const {
        return PageElementCursor{
            (page_idx_t)(offset / numElementsInAPage), (uint16_t)(offset % numElementsInAPage)};
    }

private:
    static inline void fillInMemColumnWithNonOverflowValFunc(InMemColumn* inMemColumn,
        uint8_t* defaultVal, PageByteCursor& pageByteCursor, offset_t nodeOffset,
        const DataType& dataType) {
        inMemColumn->setElement(nodeOffset, defaultVal);
    }

    static void fillInMemColumnWithStrValFunc(InMemColumn* inMemColumn, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, offset_t nodeOffset, const DataType& dataType);

    static void fillInMemColumnWithListValFunc(InMemColumn* inMemColumn, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, offset_t nodeOffset, const DataType& dataType);

    static fill_in_mem_column_function_t getFillInMemColumnFunc(const DataType& dataType);

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

class InMemRelIDColumn : public InMemColumn {

public:
    // Note: we only store the rel offset in the rel ID column since all rels in a column must share
    // the same relTableID.
    InMemRelIDColumn(string fName, uint64_t numElements)
        : InMemColumn{std::move(fName), DataType(INTERNAL_ID), sizeof(offset_t), numElements} {}
};

class InMemAdjColumn : public InMemColumn {

public:
    InMemAdjColumn(
        string fName, const NodeIDCompressionScheme& nodeIDCompressionScheme, uint64_t numElements)
        : InMemColumn{std::move(fName), DataType(INTERNAL_ID),
              nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression(), numElements},
          nodeIDCompressionScheme{nodeIDCompressionScheme} {}

    void setElement(offset_t offset, const uint8_t* val) override;

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
