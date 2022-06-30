#pragma once

#include "src/common/types/include/literal.h"
#include "src/common/types/include/value.h"
#include "src/storage/storage_structure/include/lists/large_list_handle.h"
#include "src/storage/storage_structure/include/lists/list_headers.h"
#include "src/storage/storage_structure/include/lists/lists_metadata.h"
#include "src/storage/storage_structure/include/overflow_file.h"
#include "src/storage/storage_structure/include/storage_structure.h"

namespace graphflow {
namespace loader {
class LoaderEmptyListsTest;
} // namespace loader
} // namespace graphflow

namespace graphflow {
namespace storage {

struct ListInfo {
    bool isLargeList{false};
    uint64_t listLen{-1u};
    std::function<uint32_t(uint32_t)> mapper;
    PageCursor cursor;

    inline bool isEmpty() { return listLen <= 0; }
};

/**
 * A lists data structure holds a list of homogeneous values for each offset in it. Lists are used
 * for storing Adjacency List, Rel Property Lists and unstructured Node PropertyNameDataType Lists.
 *
 * The offsets in the Lists are partitioned into fixed size. Hence, each offset, and its list,
 * belongs to a chunk. If the offset's list is small (less than the PAGE_SIZE) it is stored together
 * along with other lists in that chunk as in a CSR. However, large lists are stored out of their
 * regular chunks and span multiple pages. The nature, size and logical location of the list is
 * given by a 32-bit header value (explained in {@class ListHeaders}). Given the logical location of
 * a list, {@class ListsMetadata} contains information that maps logical location of the list to the
 * actual physical location in the Lists file on disk.
 * */
class Lists : public BaseColumnOrList {
    friend class graphflow::loader::LoaderEmptyListsTest;

public:
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory)
        : Lists{storageStructureIDAndFName, dataType, elementSize, move(headers), bufferManager,
              true /*hasNULLBytes*/, isInMemory} {};

    void readValues(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<LargeListHandle>& largeListHandle);

    ListInfo getListInfo(node_offset_t nodeOffset);

protected:
    // storageStructureIDAndFName is the ID and fName for the "main ".lists" file.
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool hasNULLBytes, bool isInMemory)
        : BaseColumnOrList{storageStructureIDAndFName, dataType, elementSize, bufferManager,
              hasNULLBytes, isInMemory, nullptr /* no wal for now */},
          storageStructureIDAndFName{storageStructureIDAndFName},
          metadata{storageStructureIDAndFName}, headers(move(headers)){};

    virtual void readFromLargeList(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<LargeListHandle>& largeListHandle, ListInfo& info);

    virtual void readSmallList(const shared_ptr<ValueVector>& valueVector, ListInfo& info);

protected:
    StorageStructureIDAndFName storageStructureIDAndFName;
    ListsMetadata metadata;
    shared_ptr<ListHeaders> headers;
};

class StringPropertyLists : public Lists {

public:
    StringPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        shared_ptr<ListHeaders> headers, BufferManager& bufferManager, bool isInMemory)
        : Lists{storageStructureIDAndFName, DataType(STRING), sizeof(gf_string_t), move(headers),
              bufferManager, isInMemory},
          stringOverflowPages{storageStructureIDAndFName, bufferManager, isInMemory,
              nullptr /* no wal for now */} {};

private:
    void readFromLargeList(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<LargeListHandle>& largeListHandle, ListInfo& info) override;

    void readSmallList(const shared_ptr<ValueVector>& valueVector, ListInfo& info) override;

private:
    OverflowFile stringOverflowPages;
};

class ListPropertyLists : public Lists {

public:
    ListPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& dataType, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory)
        : Lists{storageStructureIDAndFName, dataType, sizeof(gf_list_t), move(headers),
              bufferManager, isInMemory},
          listOverflowPages{storageStructureIDAndFName, bufferManager, isInMemory,
              nullptr /* no wal for now */} {};

private:
    void readFromLargeList(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<LargeListHandle>& largeListHandle, ListInfo& info) override;

    void readSmallList(const shared_ptr<ValueVector>& valueVector, ListInfo& info) override;

private:
    OverflowFile listOverflowPages;
};

class AdjLists : public Lists {

public:
    AdjLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        BufferManager& bufferManager, NodeIDCompressionScheme nodeIDCompressionScheme,
        bool isInMemory)
        : Lists{storageStructureIDAndFName, DataType(NODE_ID),
              nodeIDCompressionScheme.getNumTotalBytes(),
              make_shared<ListHeaders>(storageStructureIDAndFName), bufferManager, false,
              isInMemory},
          nodeIDCompressionScheme{nodeIDCompressionScheme} {};

    shared_ptr<ListHeaders> getHeaders() { return headers; };

    // Currently, used only in Loader tests.
    unique_ptr<vector<nodeID_t>> readAdjacencyListOfNode(node_offset_t nodeOffset);

private:
    void readFromLargeList(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<LargeListHandle>& largeListHandle, ListInfo& info) override;

    void readSmallList(const shared_ptr<ValueVector>& valueVector, ListInfo& info) override;

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class ListsFactory {

public:
    static unique_ptr<Lists> getLists(const StorageStructureIDAndFName& structureIDAndFName,
        const DataType& dataType, const shared_ptr<ListHeaders>& adjListsHeaders,
        BufferManager& bufferManager, bool isInMemory) {
        switch (dataType.typeID) {
        case INT64:
        case DOUBLE:
        case BOOL:
        case DATE:
        case TIMESTAMP:
        case INTERVAL:
            return make_unique<Lists>(structureIDAndFName, dataType,
                Types::getDataTypeSize(dataType), adjListsHeaders, bufferManager, isInMemory);
        case STRING:
            return make_unique<StringPropertyLists>(
                structureIDAndFName, adjListsHeaders, bufferManager, isInMemory);
        case LIST:
            return make_unique<ListPropertyLists>(
                structureIDAndFName, dataType, adjListsHeaders, bufferManager, isInMemory);
        default:
            throw StorageException("Invalid type for property list creation.");
        }
    }
};

} // namespace storage
} // namespace graphflow
