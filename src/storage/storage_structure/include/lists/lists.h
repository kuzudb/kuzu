#pragma once

#include "rel_update_store.h"

#include "src/common/types/include/literal.h"
#include "src/common/types/include/value.h"
#include "src/storage/storage_structure/include/lists/list_headers.h"
#include "src/storage/storage_structure/include/lists/list_sync_state.h"
#include "src/storage/storage_structure/include/lists/lists_metadata.h"
#include "src/storage/storage_structure/include/overflow_file.h"
#include "src/storage/storage_structure/include/storage_structure.h"

namespace graphflow {
namespace testing {
class CopyCSVEmptyListsTest;
} // namespace testing
} // namespace graphflow

namespace graphflow {
namespace storage {

struct CursorAndMapper {
    void reset(ListsMetadata& listMetadata, uint64_t numElementsPerPage, list_header_t listHeader,
        node_offset_t nodeOffset) {
        if (ListHeaders::isALargeList(listHeader)) {
            cursor = PageUtils::getPageElementCursorForPos(0, numElementsPerPage);
            mapper =
                listMetadata.getPageMapperForLargeListIdx(ListHeaders::getLargeListIdx(listHeader));
        } else {
            cursor = PageUtils::getPageElementCursorForPos(
                ListHeaders::getSmallListCSROffset(listHeader), numElementsPerPage);
            mapper =
                listMetadata.getPageMapperForChunkIdx(StorageUtils::getListChunkIdx(nodeOffset));
        }
    }
    std::function<uint32_t(uint32_t)> mapper;
    PageElementCursor cursor;
};

struct ListHandle {
    ListHandle(ListSyncState& listSyncState) : listSyncState{listSyncState} {}
    inline void resetCursorMapper(ListsMetadata& listMetadata, uint64_t numElementsPerPage) {
        cursorAndMapper.reset(listMetadata, numElementsPerPage, listSyncState.getListHeader(),
            listSyncState.getBoundNodeOffset());
    }
    inline void reset() { listSyncState.reset(); }
    ListSyncState& listSyncState;
    CursorAndMapper cursorAndMapper;
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
    friend class graphflow::testing::CopyCSVEmptyListsTest;

public:
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory, WAL* wal, shared_ptr<ListUpdateStore> listUpdateStore)
        : Lists{storageStructureIDAndFName, dataType, elementSize, move(headers), bufferManager,
              true /*hasNULLBytes*/, isInMemory, wal, listUpdateStore} {};

    inline ListsMetadata& getListsMetadata() { return metadata; };
    inline shared_ptr<ListHeaders> getHeaders() { return headers; };
    inline uint64_t getNumElementsInPersistentStore(node_offset_t nodeOffset) const {
        auto header = headers->getHeader(nodeOffset);
        return ListHeaders::isALargeList(header) ?
                   metadata.getNumElementsInLargeLists(ListHeaders::getLargeListIdx(header)) :
                   ListHeaders::getSmallListLen(header);
    }
    inline uint64_t getNumElementsInRelUpdateStore(node_offset_t nodeOffset) const {
        return listUpdateStore->getNumInsertedRelsForNodeOffset(nodeOffset);
    }
    inline uint64_t getTotalNumElementsInList(
        TransactionType transactionType, node_offset_t nodeOffset) const {
        return getNumElementsInPersistentStore(nodeOffset) +
               (transactionType == TransactionType::WRITE ?
                       getNumElementsInRelUpdateStore(nodeOffset) :
                       0);
    }
    inline uint64_t getNumElementsPerPage() const { return numElementsPerPage; }
    virtual inline void checkpointInMemoryIfNecessary() {
        cout << "Lists::checkpointInMemoryIfNecessary called." << endl;
        headers->checkpointInMemoryIfNecessary();
        metadata.checkpointInMemoryIfNecessary();
    }
    virtual inline void rollbackInMemoryIfNecessary() {
        headers->rollbackInMemoryIfNecessary();
        metadata.rollbackInMemoryIfNecessary();
    }
    void initListReadingState(
        node_offset_t nodeOffset, ListHandle& listHandle, TransactionType transactionType);

    virtual void readValues(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    virtual void readSmallList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);

protected:
    void readFromList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);

    // storageStructureIDAndFName is the ID and fName for the "main ".lists" file.
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool hasNULLBytes, bool isInMemory, WAL* wal, shared_ptr<ListUpdateStore> listUpdateStore)
        : BaseColumnOrList{storageStructureIDAndFName, dataType, elementSize, bufferManager,
              hasNULLBytes, isInMemory, wal},
          storageStructureIDAndFName{storageStructureIDAndFName},
          metadata{storageStructureIDAndFName, &bufferManager, wal}, headers{move(headers)},
          listUpdateStore{listUpdateStore} {};

    virtual void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);

protected:
    StorageStructureIDAndFName storageStructureIDAndFName;
    ListsMetadata metadata;
    shared_ptr<ListHeaders> headers;
    shared_ptr<ListUpdateStore> listUpdateStore;
};

class StringPropertyLists : public Lists {

public:
    StringPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        shared_ptr<ListHeaders> headers, BufferManager& bufferManager, bool isInMemory, WAL* wal,
        shared_ptr<ListUpdateStore> listUpdateStore)
        : Lists{storageStructureIDAndFName, DataType(STRING), sizeof(gf_string_t), move(headers),
              bufferManager, isInMemory, wal, listUpdateStore},
          stringOverflowPages{storageStructureIDAndFName, bufferManager, isInMemory, wal} {};

private:
    void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;

    void readSmallList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;

private:
    OverflowFile stringOverflowPages;
};

class ListPropertyLists : public Lists {

public:
    ListPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& dataType, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory, WAL* wal, shared_ptr<ListUpdateStore> listUpdateStore)
        : Lists{storageStructureIDAndFName, dataType, sizeof(gf_list_t), move(headers),
              bufferManager, isInMemory, wal, listUpdateStore},
          listOverflowPages{storageStructureIDAndFName, bufferManager, isInMemory, wal} {};

private:
    void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;

    void readSmallList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;

private:
    OverflowFile listOverflowPages;
};

class AdjLists : public Lists {

public:
    AdjLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        BufferManager& bufferManager, NodeIDCompressionScheme nodeIDCompressionScheme,
        bool isInMemory, WAL* wal, shared_ptr<ListUpdateStore> listUpdateStore)
        : Lists{storageStructureIDAndFName, DataType(NODE_ID),
              nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression(),
              make_shared<ListHeaders>(storageStructureIDAndFName, &bufferManager, wal),
              bufferManager, false, isInMemory, wal, listUpdateStore},
          nodeIDCompressionScheme{nodeIDCompressionScheme} {};

    void readValues(const shared_ptr<ValueVector>& valueVector, ListHandle& listSyncState) override;

    // Currently, used only in copyCSV tests.
    unique_ptr<vector<nodeID_t>> readAdjacencyListOfNode(node_offset_t nodeOffset);

private:
    void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;

    void readSmallList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;

    void readFromRelUpdateStore(
        ListSyncState& listSyncState, shared_ptr<ValueVector> valueVector) const;

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class ListsFactory {

public:
    static unique_ptr<Lists> getLists(const StorageStructureIDAndFName& structureIDAndFName,
        const DataType& dataType, const shared_ptr<ListHeaders>& adjListsHeaders,
        BufferManager& bufferManager, bool isInMemory, WAL* wal,
        shared_ptr<ListUpdateStore> listUpdateStore) {
        switch (dataType.typeID) {
        case INT64:
        case DOUBLE:
        case BOOL:
        case DATE:
        case TIMESTAMP:
        case INTERVAL:
            return make_unique<Lists>(structureIDAndFName, dataType,
                Types::getDataTypeSize(dataType), adjListsHeaders, bufferManager, isInMemory, wal,
                listUpdateStore);
        case STRING:
            return make_unique<StringPropertyLists>(structureIDAndFName, adjListsHeaders,
                bufferManager, isInMemory, wal, listUpdateStore);
        case LIST:
            return make_unique<ListPropertyLists>(structureIDAndFName, dataType, adjListsHeaders,
                bufferManager, isInMemory, wal, listUpdateStore);
        default:
            throw StorageException("Invalid type for property list creation.");
        }
    }
};

} // namespace storage
} // namespace graphflow
