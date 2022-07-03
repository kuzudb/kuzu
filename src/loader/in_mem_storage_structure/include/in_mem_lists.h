#pragma once

#include "src/storage/storage_structure/include/in_mem_file.h"
#include "src/storage/storage_structure/include/lists/list_headers.h"
#include "src/storage/storage_structure/include/lists/lists_metadata.h"

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
    static PageCursor calcPageElementCursor(uint32_t header, uint64_t reversePos,
        uint8_t numBytesPerElement, node_offset_t nodeOffset, ListsMetadataBuilder& metadataBuilder,
        bool hasNULLBytes);
};

class InMemLists {

public:
    InMemLists(string fName, DataType dataType, uint64_t numBytesForElement);

    virtual ~InMemLists() = default;

    virtual void saveToFile();
    virtual void setElement(uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val);
    virtual inline InMemOverflowFile* getOverflowPages() { return nullptr; }
    inline ListsMetadataBuilder* getListsMetadataBuilder() { return listsMetadataBuilder.get(); }
    inline uint8_t* getMemPtrToLoc(uint64_t pageIdx, uint16_t posInPage) {
        return inMemFile->getPage(pageIdx)->data + (posInPage * numBytesForElement);
    }

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
    InMemListsWithOverflow(string fName, DataType dataType);

    ~InMemListsWithOverflow() override = default;

    InMemOverflowFile* getOverflowPages() override { return overflowInMemFile.get(); }
    void saveToFile() override;

protected:
    unique_ptr<InMemOverflowFile> overflowInMemFile;
};

class InMemAdjLists : public InMemLists {

public:
    InMemAdjLists(string fName, const NodeIDCompressionScheme& compressionScheme, uint64_t numNodes)
        : InMemLists{move(fName), DataType(NODE_ID), compressionScheme.getNumTotalBytes()},
          compressionScheme{compressionScheme} {
        listHeadersBuilder = make_unique<ListHeadersBuilder>(this->fName, numNodes);
    };

    ~InMemAdjLists() override = default;

    void setElement(uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val) override;

    void saveToFile() override;

    inline ListHeadersBuilder* getListHeadersBuilder() const { return listHeadersBuilder.get(); }

private:
    unique_ptr<ListHeadersBuilder> listHeadersBuilder;
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
        listHeadersBuilder = make_unique<ListHeadersBuilder>(this->fName, numNodes);
    };

    inline ListHeadersBuilder* getListHeadersBuilder() { return listHeadersBuilder.get(); }
    inline atomic_uint64_vec_t* getListSizes() { return listSizes.get(); }

    void setUnstructuredElement(PageCursor& cursor, uint32_t propertyKey, DataTypeID dataTypeID,
        const uint8_t* val, PageCursor* overflowCursor);

    void saveToFile() override;

private:
    void setComponentOfUnstrProperty(PageCursor& localCursor, uint8_t len, const uint8_t* val);

private:
    unique_ptr<atomic_uint64_vec_t> listSizes;
    unique_ptr<ListHeadersBuilder> listHeadersBuilder;
};

class InMemListsFactory {

public:
    static unique_ptr<InMemLists> getInMemPropertyLists(
        const string& fName, const DataType& dataType, uint64_t numNodes);
};

} // namespace loader
} // namespace graphflow
