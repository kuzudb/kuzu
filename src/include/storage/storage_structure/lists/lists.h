#pragma once

#include "common/types/literal.h"
#include "lists_update_store.h"
#include "storage/storage_structure/disk_overflow_file.h"
#include "storage/storage_structure/lists/list_handle.h"
#include "storage/storage_structure/storage_structure.h"

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
    friend class ListsUpdateIterator;
    friend class ListsUpdateIteratorFactory;

public:
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory, WAL* wal, ListsUpdatesStore* listsUpdatesStore)
        : Lists{storageStructureIDAndFName, dataType, elementSize, std::move(headers),
              bufferManager, true /*hasNULLBytes*/, isInMemory, wal, listsUpdatesStore} {};
    inline ListsMetadata& getListsMetadata() { return metadata; };
    inline shared_ptr<ListHeaders> getHeaders() const { return headers; };
    // TODO(Guodong): change the input to header.
    inline uint64_t getNumElementsFromListHeader(node_offset_t nodeOffset) const {
        auto header = headers->getHeader(nodeOffset);
        return ListHeaders::isALargeList(header) ?
                   metadata.getNumElementsInLargeLists(ListHeaders::getLargeListIdx(header)) :
                   ListHeaders::getSmallListLen(header);
    }
    inline uint64_t getNumElementsInListsUpdatesStore(node_offset_t nodeOffset) {
        return listsUpdatesStore->getNumInsertedRelsForNodeOffset(
            storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset);
    }
    inline uint64_t getTotalNumElementsInList(
        TransactionType transactionType, node_offset_t nodeOffset) {
        return getNumElementsInPersistentStore(transactionType, nodeOffset) +
               (transactionType == TransactionType::WRITE ?
                       getNumElementsInListsUpdatesStore(nodeOffset) -
                           listsUpdatesStore->getNumDeletedRels(
                               storageStructureIDAndFName.storageStructureID.listFileID,
                               nodeOffset) :
                       0);
    }
    virtual inline void checkpointInMemoryIfNecessary() {
        metadata.checkpointInMemoryIfNecessary();
    }
    virtual inline void rollbackInMemoryIfNecessary() { metadata.rollbackInMemoryIfNecessary(); }
    virtual inline bool mayContainNulls() const { return true; }
    virtual inline void setDeletedRelsIfNecessary(Transaction* transaction, ListHandle& listHandle,
        const shared_ptr<ValueVector>& valueVector) {
        // DO NOTHING.
    }
    virtual void readValues(Transaction* transaction, const shared_ptr<ValueVector>& valueVector,
        ListHandle& listHandle);
    virtual void readFromSmallList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    virtual void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    void readFromList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle);
    uint64_t getNumElementsInPersistentStore(
        TransactionType transactionType, node_offset_t nodeOffset);
    void initListReadingState(
        node_offset_t nodeOffset, ListHandle& listHandle, TransactionType transactionType);
    unique_ptr<InMemList> createInMemListWithDataFromUpdateStoreOnly(
        node_offset_t nodeOffset, vector<uint64_t>& insertedRelsTupleIdxInFT);
    // This function writes the persistent store data (skipping over the deleted rels) and update
    // store data to the inMemList.
    unique_ptr<InMemList> writeToInMemList(node_offset_t nodeOffset,
        const vector<uint64_t>& insertedRelTupleIdxesInFT,
        const unordered_set<uint64_t>& deletedRelOffsetsForList,
        UpdatedPersistentListOffsets* updatedPersistentListOffsets);
    void fillInMemListsFromPersistentStore(node_offset_t nodeOffset,
        uint64_t numElementsInPersistentStore, InMemList& inMemList,
        const unordered_set<list_offset_t>& deletedRelOffsetsInList,
        UpdatedPersistentListOffsets* updatedPersistentListOffsets = nullptr);

protected:
    virtual inline DiskOverflowFile* getDiskOverflowFileIfExists() { return nullptr; }
    virtual inline NodeIDCompressionScheme* getNodeIDCompressionIfExists() { return nullptr; }
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName, const DataType& dataType,
        const size_t& elementSize, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool hasNULLBytes, bool isInMemory, WAL* wal, ListsUpdatesStore* listsUpdatesStore)
        : BaseColumnOrList{storageStructureIDAndFName, dataType, elementSize, bufferManager,
              hasNULLBytes, isInMemory, wal},
          storageStructureIDAndFName{storageStructureIDAndFName},
          metadata{storageStructureIDAndFName, &bufferManager, wal}, headers{std::move(headers)},
          listsUpdatesStore{listsUpdatesStore} {};

private:
    void fillInMemListsFromFrame(InMemList& inMemList, const uint8_t* frame, uint64_t elemPosInPage,
        uint64_t numElementsToReadInCurPage, const unordered_set<uint64_t>& deletedRelOffsetsInList,
        uint64_t numElementsRead, uint64_t& nextPosToWriteToInMemList,
        UpdatedPersistentListOffsets* updatedPersistentListOffsets);

    void readPropertyUpdatesToInMemListIfExists(InMemList& inMemList,
        UpdatedPersistentListOffsets* updatedPersistentListOffsets, uint64_t numElementsRead,
        uint64_t numElementsToReadInCurPage, uint64_t nextPosToWriteToInMemList);

protected:
    StorageStructureIDAndFName storageStructureIDAndFName;
    ListsMetadata metadata;
    shared_ptr<ListHeaders> headers;
    ListsUpdatesStore* listsUpdatesStore;
};

class PropertyListsWithOverflow : public Lists {
public:
    PropertyListsWithOverflow(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& dataType, shared_ptr<ListHeaders> headers, BufferManager& bufferManager,
        bool isInMemory, WAL* wal, ListsUpdatesStore* listsUpdatesStore)
        : Lists{storageStructureIDAndFName, dataType, Types::getDataTypeSize(dataType),
              std::move(headers), bufferManager, isInMemory, wal, listsUpdatesStore},
          diskOverflowFile{storageStructureIDAndFName, bufferManager, isInMemory, wal} {}

private:
    inline DiskOverflowFile* getDiskOverflowFileIfExists() override { return &diskOverflowFile; }

public:
    DiskOverflowFile diskOverflowFile;
};

class StringPropertyLists : public PropertyListsWithOverflow {

public:
    StringPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const shared_ptr<ListHeaders>& headers, BufferManager& bufferManager, bool isInMemory,
        WAL* wal, ListsUpdatesStore* listsUpdatesStore)
        : PropertyListsWithOverflow{storageStructureIDAndFName, DataType{STRING}, headers,
              bufferManager, isInMemory, wal, listsUpdatesStore} {};

private:
    void readFromLargeList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;
    void readFromSmallList(
        const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) override;
};

class ListPropertyLists : public PropertyListsWithOverflow {

public:
    ListPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& dataType, const shared_ptr<ListHeaders>& headers,
        BufferManager& bufferManager, bool isInMemory, WAL* wal,
        ListsUpdatesStore* listsUpdatesStore)
        : PropertyListsWithOverflow{storageStructureIDAndFName, dataType, headers, bufferManager,
              isInMemory, wal, listsUpdatesStore} {};

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
        bool isInMemory, WAL* wal, ListsUpdatesStore* listsUpdatesStore)
        : Lists{storageStructureIDAndFName, DataType(NODE_ID),
              nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression(),
              make_shared<ListHeaders>(storageStructureIDAndFName, &bufferManager, wal),
              bufferManager, false /* hasNullBytes */, isInMemory, wal, listsUpdatesStore},
          nodeIDCompressionScheme{nodeIDCompressionScheme} {};

    inline bool mayContainNulls() const override { return false; }

    void readValues(Transaction* transaction, const shared_ptr<ValueVector>& valueVector,
        ListHandle& listHandle) override;

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
    void readFromListsUpdatesStore(
        ListHandle& listHandle, const shared_ptr<ValueVector>& valueVector);
    void readFromPersistentStore(
        ListHandle& listHandle, const shared_ptr<ValueVector>& valueVector);

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class RelIDList : public Lists {

public:
    RelIDList(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& dataType, const size_t& elementSize, shared_ptr<ListHeaders> headers,
        BufferManager& bufferManager, bool isInMemory, WAL* wal,
        ListsUpdatesStore* listsUpdatesStore)
        : Lists{storageStructureIDAndFName, dataType, elementSize, std::move(headers),
              bufferManager, isInMemory, wal, listsUpdatesStore} {}
    void setDeletedRelsIfNecessary(Transaction* transaction, ListHandle& listHandle,
        const shared_ptr<ValueVector>& relIDVector) override;
    unordered_set<uint64_t> getDeletedRelOffsetsInListForNodeOffset(node_offset_t nodeOffset);
    list_offset_t getListOffset(node_offset_t nodeOffset, int64_t relID);
};

class ListsFactory {

public:
    static unique_ptr<Lists> getLists(const StorageStructureIDAndFName& structureIDAndFName,
        const DataType& dataType, const shared_ptr<ListHeaders>& adjListsHeaders,
        BufferManager& bufferManager, bool isInMemory, WAL* wal,
        ListsUpdatesStore* listsUpdatesStore) {
        assert(listsUpdatesStore != nullptr);
        // TODO(Ziyi): this is a super hacky design. Consider storing a relIDColumn/List in relTable
        // just like adjColumn/List and we can have Extend read from both relIDColumn/List and
        // adjColumn/List.
        if (structureIDAndFName.storageStructureID.listFileID.relPropertyListID.propertyID ==
            RelTableSchema::INTERNAL_REL_ID_PROPERTY_IDX) {
            return make_unique<RelIDList>(structureIDAndFName, dataType,
                Types::getDataTypeSize(dataType), adjListsHeaders, bufferManager, isInMemory, wal,
                listsUpdatesStore);
        }
        switch (dataType.typeID) {
        case INT64:
        case DOUBLE:
        case BOOL:
        case DATE:
        case TIMESTAMP:
        case INTERVAL:
            return make_unique<Lists>(structureIDAndFName, dataType,
                Types::getDataTypeSize(dataType), adjListsHeaders, bufferManager, isInMemory, wal,
                listsUpdatesStore);
        case STRING:
            return make_unique<StringPropertyLists>(structureIDAndFName, adjListsHeaders,
                bufferManager, isInMemory, wal, listsUpdatesStore);
        case LIST:
            return make_unique<ListPropertyLists>(structureIDAndFName, dataType, adjListsHeaders,
                bufferManager, isInMemory, wal, listsUpdatesStore);
        default:
            throw StorageException("Invalid type for property list creation.");
        }
    }
};

} // namespace storage
} // namespace kuzu
