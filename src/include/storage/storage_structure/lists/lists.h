#pragma once

#include "common/types/literal.h"
#include "lists_update_store.h"
#include "storage/storage_structure/disk_overflow_file.h"
#include "storage/storage_structure/lists/list_headers.h"
#include "storage/storage_structure/lists/list_sync_state.h"
#include "storage/storage_structure/lists/lists_metadata.h"
#include "storage/storage_structure/storage_structure.h"

namespace kuzu {
namespace testing {
class CopyCSVEmptyListsTest;
} // namespace testing
} // namespace kuzu

namespace kuzu {
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
 * for storing Adjacency List, Rel Property Lists.
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
    friend class kuzu::testing::CopyCSVEmptyListsTest;
    friend class ListsUpdateIterator;
    friend class ListsUpdateIteratorFactory;

public:
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory, WAL* wal, ListsUpdateStore* listsUpdateStore)
        : Lists{storageStructureIDAndFName, dataType, elementSize, move(headers), bufferManager,
              true /*hasNULLBytes*/, isInMemory, wal, listsUpdateStore} {};
    inline ListsMetadata& getListsMetadata() { return metadata; };
    inline shared_ptr<ListHeaders> getHeaders() const { return headers; };
    inline uint64_t getNumElementsFromListHeader(node_offset_t nodeOffset) const {
        auto header = headers->getHeader(nodeOffset);
        return ListHeaders::isALargeList(header) ?
                   metadata.getNumElementsInLargeLists(ListHeaders::getLargeListIdx(header)) :
                   ListHeaders::getSmallListLen(header);
    }
    inline uint64_t getNumElementsInListsUpdateStore(node_offset_t nodeOffset) {
        return listsUpdateStore->getNumInsertedRelsForNodeOffset(
            storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset);
    }
    inline uint64_t getTotalNumElementsInList(
        TransactionType transactionType, node_offset_t nodeOffset) {
        return getNumElementsInPersistentStore(transactionType, nodeOffset) +
               (transactionType == TransactionType::WRITE ?
                       getNumElementsInListsUpdateStore(nodeOffset) :
                       0);
    }
    virtual inline void checkpointInMemoryIfNecessary() {
        metadata.checkpointInMemoryIfNecessary();
    }
    virtual inline void rollbackInMemoryIfNecessary() { metadata.rollbackInMemoryIfNecessary(); }
    virtual inline bool mayContainNulls() const { return true; }
    // Prepares all the db file changes necessary to update the "persistent" store of lists with the
    // listsUpdateStore, which stores the updates by the write trx locally.
    virtual void prepareCommitOrRollbackIfNecessary(bool isCommit);
    virtual void readValues(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    virtual void readFromSmallList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    virtual void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    void readFromList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    void fillInMemListsFromPersistentStore(CursorAndMapper& cursorAndMapper,
        uint64_t numElementsInPersistentStore, InMemList& inMemList);
    uint64_t getNumElementsInPersistentStore(
        TransactionType transactionType, node_offset_t nodeOffset);
    void initListReadingState(
        node_offset_t nodeOffset, ListHandle& listHandle, TransactionType transactionType);
    unique_ptr<InMemList> getInMemListWithDataFromUpdateStoreOnly(
        node_offset_t nodeOffset, vector<uint64_t>& insertedRelsTupleIdxInFT);

protected:
    virtual inline DiskOverflowFile* getDiskOverflowFileIfExists() { return nullptr; }
    virtual inline NodeIDCompressionScheme* getNodeIDCompressionIfExists() { return nullptr; }
    // storageStructureIDAndFName is the ID and fName for the "main ".lists" file.
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool hasNULLBytes, bool isInMemory, WAL* wal, ListsUpdateStore* listsUpdateStore)
        : BaseColumnOrList{storageStructureIDAndFName, dataType, elementSize, bufferManager,
              hasNULLBytes, isInMemory, wal},
          storageStructureIDAndFName{storageStructureIDAndFName},
          metadata{storageStructureIDAndFName, &bufferManager, wal}, headers{move(headers)},
          listsUpdateStore{listsUpdateStore} {};

private:
    void prepareCommit(ListsUpdateIterator& listsUpdateIterator);

protected:
    StorageStructureIDAndFName storageStructureIDAndFName;
    ListsMetadata metadata;
    shared_ptr<ListHeaders> headers;
    ListsUpdateStore* listsUpdateStore;
};

class PropertyListsWithOverflow : public Lists {
public:
    PropertyListsWithOverflow(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& dataType, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory, WAL* wal, ListsUpdateStore* listsUpdateStore)
        : Lists{storageStructureIDAndFName, dataType, Types::getDataTypeSize(dataType),
              move(headers), bufferManager, isInMemory, wal, listsUpdateStore},
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
        ListsUpdateStore* listsUpdateStore)
        : PropertyListsWithOverflow{storageStructureIDAndFName, DataType{STRING}, headers,
              bufferManager, isInMemory, wal, listsUpdateStore} {};

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
        bool isInMemory, WAL* wal, ListsUpdateStore* listsUpdateStore)
        : PropertyListsWithOverflow{storageStructureIDAndFName, dataType, headers, bufferManager,
              isInMemory, wal, listsUpdateStore} {};

private:
    void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;
    void readFromSmallList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;
};

class AdjLists : public Lists {

public:
    AdjLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        BufferManager& bufferManager, NodeIDCompressionScheme nodeIDCompressionScheme,
        bool isInMemory, WAL* wal, ListsUpdateStore* listsUpdateStore)
        : Lists{storageStructureIDAndFName, DataType(NODE_ID),
              nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression(),
              make_shared<ListHeaders>(storageStructureIDAndFName, &bufferManager, wal),
              bufferManager, false /* hasNullBytes */, isInMemory, wal, listsUpdateStore},
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
    void readFromListsUpdateStore(
        ListSyncState& listSyncState, shared_ptr<ValueVector> valueVector);

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class ListsFactory {

public:
    static unique_ptr<Lists> getLists(const StorageStructureIDAndFName& structureIDAndFName,
        const DataType& dataType, const shared_ptr<ListHeaders>& adjListsHeaders,
        BufferManager& bufferManager, bool isInMemory, WAL* wal,
        ListsUpdateStore* listsUpdateStore) {
        switch (dataType.typeID) {
        case INT64:
        case DOUBLE:
        case BOOL:
        case DATE:
        case TIMESTAMP:
        case INTERVAL:
            return make_unique<Lists>(structureIDAndFName, dataType,
                Types::getDataTypeSize(dataType), adjListsHeaders, bufferManager, isInMemory, wal,
                listsUpdateStore);
        case STRING:
            return make_unique<StringPropertyLists>(structureIDAndFName, adjListsHeaders,
                bufferManager, isInMemory, wal, listsUpdateStore);
        case LIST:
            return make_unique<ListPropertyLists>(structureIDAndFName, dataType, adjListsHeaders,
                bufferManager, isInMemory, wal, listsUpdateStore);
        default:
            throw StorageException("Invalid type for property list creation.");
        }
    }
};

} // namespace storage
} // namespace kuzu
