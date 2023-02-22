#pragma once

#include <functional>

#include "storage/storage_structure/in_mem_file.h"

namespace kuzu {
namespace storage {

class InMemColumn;

using fill_in_mem_column_function_t = std::function<void(InMemColumn* inMemColumn,
    uint8_t* defaultVal, PageByteCursor& pageByteCursor, common::offset_t nodeOffset,
    const common::DataType& dataType)>;

class InMemColumn {

public:
    // For property columns.
    InMemColumn(std::string fName, common::DataType dataType, uint64_t numBytesForElement,
        uint64_t numElements);

    void fillWithDefaultVal(
        uint8_t* defaultVal, uint64_t numNodes, const common::DataType& dataType);

    virtual ~InMemColumn() = default;

    virtual void saveToFile();

    virtual void setElement(common::offset_t offset, const uint8_t* val);
    inline uint8_t* getElement(common::offset_t offset) {
        auto cursor = getPageElementCursorForOffset(offset);
        return inMemFile->getPage(cursor.pageIdx)->data +
               (cursor.elemPosInPage * numBytesForElement);
    }

    virtual inline InMemOverflowFile* getInMemOverflowFile() { return nullptr; }

    inline common::DataType getDataType() { return dataType; }

    inline bool isNullAtNodeOffset(common::offset_t nodeOffset) {
        auto cursor = getPageElementCursorForOffset(nodeOffset);
        return inMemFile->getPage(cursor.pageIdx)->isElemPosNull(cursor.elemPosInPage);
    }

protected:
    inline PageElementCursor getPageElementCursorForOffset(common::offset_t offset) const {
        return PageElementCursor{(common::page_idx_t)(offset / numElementsInAPage),
            (uint16_t)(offset % numElementsInAPage)};
    }

private:
    static inline void fillInMemColumnWithNonOverflowValFunc(InMemColumn* inMemColumn,
        uint8_t* defaultVal, PageByteCursor& pageByteCursor, common::offset_t nodeOffset,
        const common::DataType& dataType) {
        inMemColumn->setElement(nodeOffset, defaultVal);
    }

    static void fillInMemColumnWithStrValFunc(InMemColumn* inMemColumn, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, common::offset_t nodeOffset,
        const common::DataType& dataType);

    static void fillInMemColumnWithListValFunc(InMemColumn* inMemColumn, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, common::offset_t nodeOffset,
        const common::DataType& dataType);

    static fill_in_mem_column_function_t getFillInMemColumnFunc(const common::DataType& dataType);

protected:
    std::string fName;
    common::DataType dataType;
    uint64_t numBytesForElement;
    uint64_t numElementsInAPage;
    std::unique_ptr<InMemFile> inMemFile;
};

class InMemColumnWithOverflow : public InMemColumn {

protected:
    InMemColumnWithOverflow(std::string fName, common::DataType dataType, uint64_t numElements);

    ~InMemColumnWithOverflow() override = default;

    void saveToFile() override;

    inline InMemOverflowFile* getInMemOverflowFile() override { return inMemOverflowFile.get(); }

protected:
    std::unique_ptr<InMemOverflowFile> inMemOverflowFile;
};

class InMemRelIDColumn : public InMemColumn {

public:
    // Note: we only store the rel offset in the rel ID column since all rels in a column must share
    // the same relTableID.
    InMemRelIDColumn(std::string fName, uint64_t numElements)
        : InMemColumn{std::move(fName), common::DataType(common::INTERNAL_ID),
              sizeof(common::offset_t), numElements} {}
};

class InMemAdjColumn : public InMemColumn {

public:
    InMemAdjColumn(std::string fName, uint64_t numElements)
        : InMemColumn{std::move(fName), common::DataType(common::INTERNAL_ID),
              sizeof(common::offset_t), numElements} {}

    void setElement(common::offset_t offset, const uint8_t* val) override;
};

class InMemStringColumn : public InMemColumnWithOverflow {

public:
    InMemStringColumn(std::string fName, uint64_t numElements)
        : InMemColumnWithOverflow{std::move(fName), common::DataType(common::STRING), numElements} {
    }
};

class InMemListColumn : public InMemColumnWithOverflow {

public:
    InMemListColumn(std::string fName, common::DataType dataType, uint64_t numElements)
        : InMemColumnWithOverflow{std::move(fName), std::move(dataType), numElements} {
        assert(this->dataType.typeID == common::VAR_LIST);
    };
};

class InMemColumnFactory {

public:
    static std::unique_ptr<InMemColumn> getInMemPropertyColumn(
        const std::string& fName, const common::DataType& dataType, uint64_t numElements);
};

} // namespace storage
} // namespace kuzu
