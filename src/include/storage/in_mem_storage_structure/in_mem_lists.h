#pragma once

#include "storage/storage_structure/in_mem_file.h"
#include "storage/storage_structure/lists/list_headers.h"
#include "storage/storage_structure/lists/lists_metadata.h"

namespace kuzu {
namespace storage {

typedef vector<atomic<uint64_t>> atomic_uint64_vec_t;

class InMemLists;
class AdjLists;

using fill_in_mem_lists_function_t = std::function<void(InMemLists* inMemLists, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, node_offset_t nodeOffset, list_header_t header,
    uint64_t posInList, const DataType& dataType)>;

class InMemListsUtils {

public:
    static inline void incrementListSize(
        atomic_uint64_vec_t& listSizes, uint32_t offset, uint32_t val) {
        assert(offset < listSizes.size());
        listSizes[offset].fetch_add(val, memory_order_relaxed);
    }
    static inline uint64_t decrementListSize(
        atomic_uint64_vec_t& listSizes, uint32_t offset, uint32_t val) {
        assert(offset < listSizes.size());
        return listSizes[offset].fetch_sub(val, memory_order_relaxed);
    }

    // Calculates the page id and offset in page where the data of a particular list has to be put
    // in the in-mem pages.
    static PageElementCursor calcPageElementCursor(uint32_t header, uint64_t reversePos,
        uint8_t numBytesPerElement, node_offset_t nodeOffset, ListsMetadataBuilder& metadataBuilder,
        bool hasNULLBytes);
};

class InMemLists {

public:
    InMemLists(string fName, DataType dataType, uint64_t numBytesForElement, uint64_t numNodes);

    void fillWithDefaultVal(
        uint8_t* defaultVal, uint64_t numNodes, AdjLists* adjList, const DataType& dataType);

    virtual ~InMemLists() = default;

    virtual void saveToFile();
    virtual void setElement(uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val);
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
        uint8_t* defaultVal, PageByteCursor& pageByteCursor, node_offset_t nodeOffset,
        list_header_t header, uint64_t posInList, const DataType& dataType) {
        inMemLists->setElement(header, nodeOffset, posInList, defaultVal);
    }

    static void fillInMemListsWithStrValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, node_offset_t nodeOffset, list_header_t header,
        uint64_t posInList, const DataType& dataType);

    static void fillInMemListsWithListValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, node_offset_t nodeOffset, list_header_t header,
        uint64_t posInList, const DataType& dataType);

    static fill_in_mem_lists_function_t getFillInMemListsFunc(const DataType& dataType);

public:
    unique_ptr<InMemFile> inMemFile;

protected:
    string fName;
    DataType dataType;
    uint64_t numBytesForElement;
    unique_ptr<ListsMetadataBuilder> listsMetadataBuilder;
};

class InMemListsWithOverflow : public InMemLists {

protected:
    InMemListsWithOverflow(string fName, DataType dataType, uint64_t numNodes);

    ~InMemListsWithOverflow() override = default;

    InMemOverflowFile* getInMemOverflowFile() override { return overflowInMemFile.get(); }
    void saveToFile() override;

protected:
    unique_ptr<InMemOverflowFile> overflowInMemFile;
};

class InMemAdjLists : public InMemLists {

public:
    InMemAdjLists(
        string fName, const NodeIDCompressionScheme& nodeIDCompressionScheme, uint64_t numNodes)
        : InMemLists{move(fName), DataType(NODE_ID),
              nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression(), numNodes},
          nodeIDCompressionScheme{nodeIDCompressionScheme} {
        listHeadersBuilder = make_unique<ListHeadersBuilder>(this->fName, numNodes);
    };

    ~InMemAdjLists() override = default;

    void setElement(uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val) override;

    void saveToFile() override;

    inline ListHeadersBuilder* getListHeadersBuilder() const { return listHeadersBuilder.get(); }

private:
    unique_ptr<ListHeadersBuilder> listHeadersBuilder;
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class InMemStringLists : public InMemListsWithOverflow {

public:
    explicit InMemStringLists(string fName, uint64_t numNodes)
        : InMemListsWithOverflow{move(fName), DataType(STRING), numNodes} {};
};

class InMemListLists : public InMemListsWithOverflow {

public:
    InMemListLists(string fName, DataType dataType, uint64_t numNodes)
        : InMemListsWithOverflow{move(fName), move(dataType), numNodes} {};
};

class InMemListsFactory {

public:
    static unique_ptr<InMemLists> getInMemPropertyLists(
        const string& fName, const DataType& dataType, uint64_t numNodes);
};

} // namespace storage
} // namespace kuzu
