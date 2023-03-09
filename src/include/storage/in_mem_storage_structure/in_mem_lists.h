#pragma once

#include "storage/storage_structure/in_mem_file.h"
#include "storage/storage_structure/lists/list_headers.h"
#include "storage/storage_structure/lists/lists_metadata.h"

namespace kuzu {
namespace storage {

typedef std::vector<std::atomic<uint64_t>> atomic_uint64_vec_t;

class InMemLists;
class AdjLists;

using fill_in_mem_lists_function_t = std::function<void(InMemLists* inMemLists, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, common::offset_t nodeOffset, common::list_header_t header,
    uint64_t posInList, const common::DataType& dataType)>;

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

    // Calculates the page id and offset in page where the data of a particular list has to be put
    // in the in-mem pages.
    static PageElementCursor calcPageElementCursor(uint32_t header, uint64_t reversePos,
        uint8_t numBytesPerElement, common::offset_t nodeOffset,
        ListsMetadataBuilder& metadataBuilder, bool hasNULLBytes);
};

class InMemLists {

public:
    InMemLists(std::string fName, common::DataType dataType, uint64_t numBytesForElement,
        uint64_t numNodes);

    void fillWithDefaultVal(uint8_t* defaultVal, uint64_t numNodes, AdjLists* adjList,
        const common::DataType& dataType);

    virtual ~InMemLists() = default;

    virtual void saveToFile();
    virtual void setElement(
        uint32_t header, common::offset_t nodeOffset, uint64_t pos, uint8_t* val);
    virtual inline InMemOverflowFile* getInMemOverflowFile() { return nullptr; }
    inline ListsMetadataBuilder* getListsMetadataBuilder() { return listsMetadataBuilder.get(); }
    inline uint8_t* getMemPtrToLoc(uint64_t pageIdx, uint16_t posInPage) {
        return inMemFile->getPage(pageIdx)->data + (posInPage * numBytesForElement);
    }

    void initListsMetadataAndAllocatePages(
        uint64_t numNodes, ListHeaders* listHeaders, ListsMetadata* listsMetadata);

private:
    void initLargeListPageLists(uint64_t numNodes, ListHeaders* listHeaders);

    void allocatePagesForLargeList(
        uint64_t numElementsInList, uint64_t numElementsPerPage, uint32_t& largeListIdx);

    void calculatePagesForSmallList(uint64_t& numPages, uint64_t& offsetInPage,
        uint64_t numElementsInList, uint64_t numElementsPerPage);

    static inline void fillInMemListsWithNonOverflowValFunc(InMemLists* inMemLists,
        uint8_t* defaultVal, PageByteCursor& pageByteCursor, common::offset_t nodeOffset,
        common::list_header_t header, uint64_t posInList, const common::DataType& dataType) {
        inMemLists->setElement(header, nodeOffset, posInList, defaultVal);
    }

    static void fillInMemListsWithStrValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, common::offset_t nodeOffset, common::list_header_t header,
        uint64_t posInList, const common::DataType& dataType);

    static void fillInMemListsWithListValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, common::offset_t nodeOffset, common::list_header_t header,
        uint64_t posInList, const common::DataType& dataType);

    static fill_in_mem_lists_function_t getFillInMemListsFunc(const common::DataType& dataType);

public:
    std::unique_ptr<InMemFile> inMemFile;

protected:
    std::string fName;
    common::DataType dataType;
    uint64_t numBytesForElement;
    std::unique_ptr<ListsMetadataBuilder> listsMetadataBuilder;
};

class InMemRelIDLists : public InMemLists {
public:
    InMemRelIDLists(std::string fName, uint64_t numNodes)
        : InMemLists{std::move(fName), common::DataType{common::INTERNAL_ID},
              sizeof(common::offset_t), numNodes} {}
};

class InMemListsWithOverflow : public InMemLists {

protected:
    InMemListsWithOverflow(std::string fName, common::DataType dataType, uint64_t numNodes);

    InMemOverflowFile* getInMemOverflowFile() override { return overflowInMemFile.get(); }
    void saveToFile() override;

protected:
    std::unique_ptr<InMemOverflowFile> overflowInMemFile;
};

class InMemAdjLists : public InMemLists {

public:
    InMemAdjLists(std::string fName, uint64_t numNodes)
        : InMemLists{std::move(fName), common::DataType(common::INTERNAL_ID),
              sizeof(common::offset_t), numNodes} {
        listHeadersBuilder = make_unique<ListHeadersBuilder>(this->fName, numNodes);
    };

    void setElement(
        uint32_t header, common::offset_t nodeOffset, uint64_t pos, uint8_t* val) override;

    void saveToFile() override;

    inline ListHeadersBuilder* getListHeadersBuilder() const { return listHeadersBuilder.get(); }

private:
    std::unique_ptr<ListHeadersBuilder> listHeadersBuilder;
};

class InMemStringLists : public InMemListsWithOverflow {

public:
    InMemStringLists(std::string fName, uint64_t numNodes)
        : InMemListsWithOverflow{std::move(fName), common::DataType(common::STRING), numNodes} {};
};

class InMemListLists : public InMemListsWithOverflow {

public:
    InMemListLists(std::string fName, common::DataType dataType, uint64_t numNodes)
        : InMemListsWithOverflow{std::move(fName), std::move(dataType), numNodes} {};
};

class InMemListsFactory {

public:
    static std::unique_ptr<InMemLists> getInMemPropertyLists(
        const std::string& fName, const common::DataType& dataType, uint64_t numNodes);
};

} // namespace storage
} // namespace kuzu
