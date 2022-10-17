#pragma once

#include "adj_and_property_lists_update_store.h"

#include "src/common/types/include/literal.h"
#include "src/common/types/include/value.h"
#include "src/storage/storage_structure/include/disk_overflow_file.h"
#include "src/storage/storage_structure/include/lists/list_headers.h"
#include "src/storage/storage_structure/include/lists/list_sync_state.h"
#include "src/storage/storage_structure/include/lists/lists_metadata.h"
#include "src/storage/storage_structure/include/storage_structure.h"

namespace graphflow {
namespace testing {
class CopyCSVEmptyListsTest;
} // namespace testing
} // namespace graphflow

namespace graphflow {
namespace storage {

struct InMemList {
    InMemList(uint64_t numElements, uint64_t elementSize, bool requireNullMask)
        : numElements{numElements} {
        listData = make_unique<uint8_t[]>(numElements * elementSize);
        nullMask = requireNullMask ?
                       make_unique<NullMask>(NullMask::getNumNullEntries(numElements)) :
                       nullptr;
    }

    inline uint8_t* getListData() const { return listData.get(); }
    inline bool hasNullBuffer() const { return nullMask != nullptr; }
    inline uint64_t* getNullMask() const { return nullMask->getData(); }

    uint64_t numElements;
    unique_ptr<uint8_t[]> listData;
    unique_ptr<NullMask> nullMask;
};

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
    explicit ListHandle(ListSyncState& listSyncState) : listSyncState{listSyncState} {}

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
    friend class ListsUpdateIterator;
    friend class ListsUpdateIteratorFactory;

public:
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory, WAL* wal)
        : Lists{storageStructureIDAndFName, dataType, elementSize, move(headers), bufferManager,
              true /*hasNULLBytes*/, isInMemory, wal} {};
    inline ListsMetadata& getListsMetadata() { return metadata; };
    inline shared_ptr<ListHeaders> getHeaders() { return headers; };
    inline uint64_t getNumElementsInPersistentStore(node_offset_t nodeOffset) const {
        auto header = headers->getHeader(nodeOffset);
        return ListHeaders::isALargeList(header) ?
                   metadata.getNumElementsInLargeLists(ListHeaders::getLargeListIdx(header)) :
                   ListHeaders::getSmallListLen(header);
    }
    virtual inline void checkpointInMemoryIfNecessary() {
        metadata.checkpointInMemoryIfNecessary();
    }
    virtual inline void rollbackInMemoryIfNecessary() { metadata.rollbackInMemoryIfNecessary(); }
    virtual inline bool mayContainNulls() const { return true; }
    // Prepares all the db file changes necessary to update the "persistent" store of lists with the
    // adjAndPropertyListsUpdateStore, which stores the updates by the write trx locally.
    virtual void prepareCommitOrRollbackIfNecessary(bool isCommit);
    void fillInMemListsFromPersistentStore(CursorAndMapper& cursorAndMapper,
        uint64_t numElementsInPersistentStore, InMemList& inMemList);

protected:
    virtual inline DiskOverflowFile* getDiskOverflowFileIfExists() { return nullptr; }
    virtual inline NodeIDCompressionScheme* getNodeIDCompressionIfExists() { return nullptr; }
    virtual void prepareCommit(ListsUpdateIterator& listsUpdateIterator) = 0;

    // storageStructureIDAndFName is the ID and fName for the "main ".lists" file.
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool hasNULLBytes, bool isInMemory, WAL* wal)
        : BaseColumnOrList{storageStructureIDAndFName, dataType, elementSize, bufferManager,
              hasNULLBytes, isInMemory, wal},
          storageStructureIDAndFName{storageStructureIDAndFName},
          metadata{storageStructureIDAndFName, &bufferManager, wal}, headers{move(headers)} {};

protected:
    StorageStructureIDAndFName storageStructureIDAndFName;
    ListsMetadata metadata;
    shared_ptr<ListHeaders> headers;
};

class ListsWithAdjAndPropertyListsUpdateStore : public Lists {

public:
    ListsWithAdjAndPropertyListsUpdateStore(
        const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory, WAL* wal, AdjAndPropertyListsUpdateStore* adjAndPropertyListsUpdateStore)
        : ListsWithAdjAndPropertyListsUpdateStore{storageStructureIDAndFName, dataType, elementSize,
              move(headers), bufferManager, true /*hasNULLBytes*/, isInMemory, wal,
              adjAndPropertyListsUpdateStore} {};

    inline uint64_t getNumElementsInAdjAndPropertyListsUpdateStore(node_offset_t nodeOffset) {
        return adjAndPropertyListsUpdateStore->getNumInsertedRelsForNodeOffset(
            storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset);
    }
    inline uint64_t getTotalNumElementsInList(
        TransactionType transactionType, node_offset_t nodeOffset) {
        return getNumElementsInPersistentStore(nodeOffset) +
               (transactionType == TransactionType::WRITE ?
                       getNumElementsInAdjAndPropertyListsUpdateStore(nodeOffset) :
                       0);
    }
    virtual void readValues(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    void initListReadingState(
        node_offset_t nodeOffset, ListHandle& listHandle, TransactionType transactionType);

protected:
    virtual void readFromSmallList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    virtual void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    void readFromList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    void prepareCommit(ListsUpdateIterator& listsUpdateIterator) override;

    ListsWithAdjAndPropertyListsUpdateStore(
        const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool hasNULLBytes, bool isInMemory, WAL* wal,
        AdjAndPropertyListsUpdateStore* adjAndPropertyListsUpdateStore)
        : Lists{storageStructureIDAndFName, dataType, elementSize, headers, bufferManager,
              hasNULLBytes, isInMemory, wal},
          adjAndPropertyListsUpdateStore{adjAndPropertyListsUpdateStore} {};

protected:
    AdjAndPropertyListsUpdateStore* adjAndPropertyListsUpdateStore;
};

class PropertyListsWithOverflow : public ListsWithAdjAndPropertyListsUpdateStore {
public:
    PropertyListsWithOverflow(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& dataType, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory, WAL* wal, AdjAndPropertyListsUpdateStore* adjAndPropertyListsUpdateStore)
        : ListsWithAdjAndPropertyListsUpdateStore{storageStructureIDAndFName, dataType,
              Types::getDataTypeSize(dataType), move(headers), bufferManager, isInMemory, wal,
              adjAndPropertyListsUpdateStore},
          diskOverflowFile{storageStructureIDAndFName, bufferManager, isInMemory, wal} {}

private:
    inline DiskOverflowFile* getDiskOverflowFileIfExists() override { return &diskOverflowFile; }

public:
    DiskOverflowFile diskOverflowFile;
};

class StringPropertyLists : public PropertyListsWithOverflow {

public:
    StringPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        shared_ptr<ListHeaders> headers, BufferManager& bufferManager, bool isInMemory, WAL* wal,
        AdjAndPropertyListsUpdateStore* adjAndPropertyListsUpdateStore)
        : PropertyListsWithOverflow{storageStructureIDAndFName, DataType{STRING}, headers,
              bufferManager, isInMemory, wal, adjAndPropertyListsUpdateStore} {};

private:
    void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;
    void readFromSmallList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;
};

class ListPropertyLists : public PropertyListsWithOverflow {

public:
    ListPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& dataType, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory, WAL* wal, AdjAndPropertyListsUpdateStore* adjAndPropertyListsUpdateStore)
        : PropertyListsWithOverflow{storageStructureIDAndFName, dataType, headers, bufferManager,
              isInMemory, wal, adjAndPropertyListsUpdateStore} {};

private:
    void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;
    void readFromSmallList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;
};

class AdjLists : public ListsWithAdjAndPropertyListsUpdateStore {

public:
    AdjLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        BufferManager& bufferManager, NodeIDCompressionScheme nodeIDCompressionScheme,
        bool isInMemory, WAL* wal, AdjAndPropertyListsUpdateStore* adjAndPropertyListsUpdateStore)
        : ListsWithAdjAndPropertyListsUpdateStore{storageStructureIDAndFName, DataType(NODE_ID),
              nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression(),
              make_shared<ListHeaders>(storageStructureIDAndFName, &bufferManager, wal),
              bufferManager, false, isInMemory, wal, adjAndPropertyListsUpdateStore},
          nodeIDCompressionScheme{nodeIDCompressionScheme} {};

    inline bool mayContainNulls() const override { return false; }

    void readValues(const shared_ptr<ValueVector>& valueVector, ListHandle& listSyncState) override;

    // Currently, used only in copyCSV tests.
    unique_ptr<vector<nodeID_t>> readAdjacencyListOfNode(node_offset_t nodeOffset);

    void checkpointInMemoryIfNecessary() override {
        headers->checkpointInMemoryIfNecessary();
        Lists::checkpointInMemoryIfNecessary();
    }

    void rollbackInMemoryIfNecessary() override {
        headers->rollbackInMemoryIfNecessary();
        Lists::rollbackInMemoryIfNecessary();
    }

private:
    inline NodeIDCompressionScheme* getNodeIDCompressionIfExists() override {
        return &nodeIDCompressionScheme;
    }

    void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;
    void readFromSmallList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;
    void readFromAdjAndPropertyListsUpdateStore(
        ListSyncState& listSyncState, shared_ptr<ValueVector> valueVector);

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class ListsFactory {

public:
    static unique_ptr<ListsWithAdjAndPropertyListsUpdateStore>
    getListsWithAdjAndPropertyListsUpdateStore(
        const StorageStructureIDAndFName& structureIDAndFName, const DataType& dataType,
        const shared_ptr<ListHeaders>& adjListsHeaders, BufferManager& bufferManager,
        bool isInMemory, WAL* wal, AdjAndPropertyListsUpdateStore* adjAndPropertyListsUpdateStore) {
        switch (dataType.typeID) {
        case INT64:
        case DOUBLE:
        case BOOL:
        case DATE:
        case TIMESTAMP:
        case INTERVAL:
            return make_unique<ListsWithAdjAndPropertyListsUpdateStore>(structureIDAndFName,
                dataType, Types::getDataTypeSize(dataType), adjListsHeaders, bufferManager,
                isInMemory, wal, adjAndPropertyListsUpdateStore);
        case STRING:
            return make_unique<StringPropertyLists>(structureIDAndFName, adjListsHeaders,
                bufferManager, isInMemory, wal, adjAndPropertyListsUpdateStore);
        case LIST:
            return make_unique<ListPropertyLists>(structureIDAndFName, dataType, adjListsHeaders,
                bufferManager, isInMemory, wal, adjAndPropertyListsUpdateStore);
        default:
            throw StorageException("Invalid type for property list creation.");
        }
    }
};

} // namespace storage
} // namespace graphflow
