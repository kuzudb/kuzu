#pragma once

#include "common/copier_config/copier_config.h"
#include "storage/storage_structure/in_mem_file.h"
#include "storage/storage_structure/lists/list_headers.h"
#include "storage/storage_structure/lists/lists_metadata.h"
#include <arrow/array.h>

namespace kuzu {
namespace storage {

typedef std::vector<std::atomic<uint64_t>> atomic_uint64_vec_t;

class InMemLists;
class AdjLists;

using fill_in_mem_lists_function_t =
    std::function<void(InMemLists* inMemLists, uint8_t* defaultVal, PageByteCursor& pageByteCursor,
        common::offset_t nodeOffset, uint64_t posInList, const common::LogicalType& dataType)>;

class InMemListsUtils {
public:
    static inline void incrementListSize(
        atomic_uint64_vec_t& listSizes, uint32_t offset, uint32_t val) {
        assert(offset < listSizes.size());
        listSizes[offset].fetch_add(val, std::memory_order_relaxed);
    }
    static inline uint64_t decrementListSize(
        atomic_uint64_vec_t& listSizes, uint32_t offset, uint32_t val) {
        assert(offset < listSizes.size());
        return listSizes[offset].fetch_sub(val, std::memory_order_relaxed);
    }
};

class InMemLists {
public:
    InMemLists(std::string fName, common::LogicalType dataType, uint64_t numBytesForElement,
        uint64_t numNodes, std::shared_ptr<ListHeadersBuilder> listHeadersBuilder,
        const common::CopyDescription* copyDescription, bool hasNullBytes)
        : InMemLists{std::move(fName), std::move(dataType), numBytesForElement, numNodes,
              copyDescription, hasNullBytes} {
        this->listHeadersBuilder = std::move(listHeadersBuilder);
    }
    virtual ~InMemLists() = default;

    virtual void saveToFile();
    void setValue(common::offset_t nodeOffset, uint64_t pos, uint8_t* val);
    template<typename T>
    void setValueFromString(
        common::offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length);

    virtual inline InMemOverflowFile* getInMemOverflowFile() { return nullptr; }
    inline ListsMetadataBuilder* getListsMetadataBuilder() { return listsMetadataBuilder.get(); }
    inline uint8_t* getMemPtrToLoc(uint64_t pageIdx, uint16_t posInPage) const {
        return inMemFile->getPage(pageIdx)->data + (posInPage * numBytesForElement);
    }
    inline common::LogicalType getDataType() { return dataType; }
    inline uint64_t getNumElementsInAPage() const { return numElementsInAPage; }

    virtual void copyArrowArray(
        arrow::Array* boundNodeOffsets, arrow::Array* posInRelLists, arrow::Array* array);
    template<typename T>
    void templateCopyArrayToRelLists(
        arrow::Array* boundNodeOffsets, arrow::Array* posInRelList, arrow::Array* array);
    template<typename T>
    void templateCopyArrayAsStringToRelLists(
        arrow::Array* boundNodeOffsets, arrow::Array* posInRelList, arrow::Array* array);

    void fillWithDefaultVal(uint8_t* defaultVal, uint64_t numNodes, ListHeaders* listHeaders);
    void initListsMetadataAndAllocatePages(
        uint64_t numNodes, ListHeaders* listHeaders, ListsMetadata* listsMetadata);

    // Calculates the page id and offset in page where the data of a particular list has to be put
    // in the in-mem pages.
    PageElementCursor calcPageElementCursor(
        uint64_t reversePos, uint8_t numBytesPerElement, common::offset_t nodeOffset);

protected:
    InMemLists(std::string fName, common::LogicalType dataType, uint64_t numBytesForElement,
        uint64_t numNodes, const common::CopyDescription* copyDescription, bool hasNullBytes);

private:
    static void calculatePagesForList(uint64_t& numPages, uint64_t& offsetInPage,
        uint64_t numElementsInList, uint64_t numElementsPerPage);

    static inline void fillInMemListsWithNonOverflowValFunc(InMemLists* inMemLists,
        uint8_t* defaultVal, PageByteCursor& pageByteCursor, common::offset_t nodeOffset,
        uint64_t posInList, const common::LogicalType& dataType) {
        inMemLists->setValue(nodeOffset, posInList, defaultVal);
    }
    static void fillInMemListsWithStrValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, common::offset_t nodeOffset, uint64_t posInList,
        const common::LogicalType& dataType);
    static void fillInMemListsWithListValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, common::offset_t nodeOffset, uint64_t posInList,
        const common::LogicalType& dataType);
    static fill_in_mem_lists_function_t getFillInMemListsFunc(const common::LogicalType& dataType);

public:
    std::unique_ptr<InMemFile> inMemFile;

protected:
    std::string fName;
    common::LogicalType dataType;
    uint64_t numBytesForElement;
    uint64_t numElementsInAPage;
    std::unique_ptr<ListsMetadataBuilder> listsMetadataBuilder;
    std::shared_ptr<ListHeadersBuilder> listHeadersBuilder;
    const common::CopyDescription* copyDescription;
};

class InMemRelIDLists : public InMemLists {
public:
    InMemRelIDLists(std::string fName, uint64_t numNodes,
        std::shared_ptr<ListHeadersBuilder> listHeadersBuilder)
        : InMemLists{std::move(fName), common::LogicalType{common::LogicalTypeID::INTERNAL_ID},
              sizeof(common::offset_t), numNodes, std::move(listHeadersBuilder),
              nullptr /* copyDescriptor */, true /* hasNullBytes */} {}
};

class InMemListsWithOverflow : public InMemLists {
protected:
    InMemListsWithOverflow(std::string fName, common::LogicalType dataType, uint64_t numNodes,
        std::shared_ptr<ListHeadersBuilder> listHeadersBuilder,
        const common::CopyDescription* copyDescription);

    void copyArrowArray(
        arrow::Array* boundNodeOffsets, arrow::Array* posInRelLists, arrow::Array* array) final;
    template<typename T>
    void templateCopyArrayAsStringToRelListsWithOverflow(
        arrow::Array* boundNodeOffsets, arrow::Array* posInRelList, arrow::Array* array);
    template<typename T>
    void setValueFromStringWithOverflow(
        common::offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length) {
        assert(false);
    }

    inline InMemOverflowFile* getInMemOverflowFile() override { return overflowInMemFile.get(); }
    void saveToFile() override;

protected:
    std::unique_ptr<InMemOverflowFile> overflowInMemFile;
    // TODO(Guodong/Ziyi): Fix for concurrent writes.
    PageByteCursor overflowCursor;
    std::unique_ptr<uint8_t[]> blobBuffer;
};

class InMemAdjLists : public InMemLists {
public:
    InMemAdjLists(std::string fName, uint64_t numNodes)
        : InMemLists{std::move(fName), common::LogicalType(common::LogicalTypeID::INTERNAL_ID),
              sizeof(common::offset_t), numNodes, nullptr, false} {
        listHeadersBuilder = make_shared<ListHeadersBuilder>(this->fName, numNodes);
    };

    void saveToFile() override;

    inline std::shared_ptr<ListHeadersBuilder> getListHeadersBuilder() const {
        return listHeadersBuilder;
    }
    inline uint32_t getListSize(common::offset_t nodeOffset) const {
        return listHeadersBuilder->getListSize(nodeOffset);
    }
};

class InMemStringLists : public InMemListsWithOverflow {
public:
    InMemStringLists(std::string fName, uint64_t numNodes,
        std::shared_ptr<ListHeadersBuilder> listHeadersBuilder)
        : InMemListsWithOverflow{std::move(fName),
              common::LogicalType(common::LogicalTypeID::STRING), numNodes,
              std::move(listHeadersBuilder), nullptr} {};
};

class InMemListLists : public InMemListsWithOverflow {
public:
    InMemListLists(std::string fName, common::LogicalType dataType, uint64_t numNodes,
        std::shared_ptr<ListHeadersBuilder> listHeadersBuilder,
        const common::CopyDescription* copyDescription)
        : InMemListsWithOverflow{std::move(fName), std::move(dataType), numNodes,
              std::move(listHeadersBuilder), copyDescription} {};
};

class InMemListsFactory {
public:
    static std::unique_ptr<InMemLists> getInMemPropertyLists(const std::string& fName,
        const common::LogicalType& dataType, uint64_t numNodes,
        const common::CopyDescription* copyDescription,
        std::shared_ptr<ListHeadersBuilder> listHeadersBuilder = nullptr);
};

template<>
void InMemLists::templateCopyArrayToRelLists<bool>(
    arrow::Array* boundNodeOffsets, arrow::Array* posInRelList, arrow::Array* array);

// BOOL
template<>
void InMemLists::setValueFromString<bool>(
    common::offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length);
// FIXED_LIST
template<>
void InMemLists::setValueFromString<uint8_t*>(
    common::offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length);
// INTERVAL
template<>
void InMemLists::setValueFromString<common::interval_t>(
    common::offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length);
// DATE
template<>
void InMemLists::setValueFromString<common::date_t>(
    common::offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length);
// TIMESTAMP
template<>
void InMemLists::setValueFromString<common::timestamp_t>(
    common::offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length);

template<>
void InMemListsWithOverflow::setValueFromStringWithOverflow<common::ku_string_t>(
    common::offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length);
template<>
void InMemListsWithOverflow::setValueFromStringWithOverflow<common::ku_list_t>(
    common::offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length);

} // namespace storage
} // namespace kuzu
