#pragma once

#include "src/loader/include/in_mem_structure/in_mem_pages.h"
#include "src/storage/include/storage_structure/lists/list_headers.h"
#include "src/storage/include/storage_structure/lists/lists_metadata.h"
#include "src/storage/include/storage_structure/overflow_pages.h"

namespace graphflow {
namespace loader {

typedef vector<atomic<uint64_t>> atomic_uint64_vec_t;

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
        uint8_t numBytesPerElement, node_offset_t nodeOffset, ListsMetadata& metadata,
        bool hasNULLBytes);
};

class InMemLists {

public:
    InMemLists(string fName, DataType dataType, uint64_t numBytesForElement);

    virtual ~InMemLists() = default;

    virtual void saveToFile();
    virtual void setElement(uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val);
    virtual inline InMemOverflowPages* getOverflowPages() { return nullptr; }
    inline ListsMetadata* getListsMetadata() { return listsMetadata.get(); }
    inline uint8_t* getMemPtrToLoc(uint64_t pageIdx, uint16_t posInPage) {
        return pages->pages[pageIdx]->data + (posInPage * numBytesForElement);
    }

    void init();

protected:
    string fName;
    DataType dataType;
    uint64_t numBytesForElement;
    unique_ptr<ListsMetadata> listsMetadata;
    unique_ptr<InMemPages> pages;
};

class InMemListsWithOverflow : public InMemLists {

protected:
    InMemListsWithOverflow(string fName, DataType dataType);

    ~InMemListsWithOverflow() override = default;

    InMemOverflowPages* getOverflowPages() override { return overflowPages.get(); }
    void saveToFile() override;

protected:
    unique_ptr<InMemOverflowPages> overflowPages;
};

class InMemAdjLists : public InMemLists {

public:
    InMemAdjLists(string fName, const NodeIDCompressionScheme& compressionScheme, uint64_t numNodes)
        : InMemLists{move(fName), DataType(NODE), compressionScheme.getNumTotalBytes()},
          compressionScheme{compressionScheme} {
        listHeaders = make_unique<ListHeaders>(numNodes);
    };

    ~InMemAdjLists() override = default;

    void setElement(uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val) override;

    void saveToFile() override;

    inline ListHeaders* getListHeaders() const { return listHeaders.get(); }

private:
    unique_ptr<ListHeaders> listHeaders;
    NodeIDCompressionScheme compressionScheme;
};

class InMemStringLists : public InMemListsWithOverflow {

public:
    explicit InMemStringLists(string fName)
        : InMemListsWithOverflow{move(fName), DataType(STRING)} {};
};

class InMemListLists : public InMemListsWithOverflow {

public:
    InMemListLists(string fName, DataType dataType)
        : InMemListsWithOverflow{move(fName), move(dataType)} {};
};

class InMemUnstructuredLists : public InMemListsWithOverflow {

public:
    InMemUnstructuredLists(string fName, uint64_t numNodes)
        : InMemListsWithOverflow{move(fName), DataType(UNSTRUCTURED)} {
        listSizes = make_unique<atomic_uint64_vec_t>(numNodes);
        listHeaders = make_unique<ListHeaders>(numNodes);
    };

    inline ListHeaders* getListHeaders() { return listHeaders.get(); }
    inline atomic_uint64_vec_t* getListSizes() { return listSizes.get(); }

    void setUnstructuredElement(PageByteCursor& cursor, uint32_t propertyKey, DataTypeID dataTypeID,
        const uint8_t* val, PageByteCursor* overflowCursor);

    void saveToFile() override;

private:
    void setComponentOfUnstrProperty(PageByteCursor& localCursor, uint8_t len, const uint8_t* val);

private:
    unique_ptr<atomic_uint64_vec_t> listSizes;
    unique_ptr<ListHeaders> listHeaders;
};

class InMemListsFactory {

public:
    static unique_ptr<InMemLists> getInMemPropertyLists(
        const string& fName, const DataType& dataType, uint64_t numNodes);
};

} // namespace loader
} // namespace graphflow
