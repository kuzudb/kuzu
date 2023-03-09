#pragma once

#include "common/types/value.h"
#include "lists_update_store.h"
#include "storage/storage_structure/disk_overflow_file.h"
#include "storage/storage_structure/lists/list_handle.h"
#include "storage/storage_structure/storage_structure.h"

namespace kuzu {
namespace storage {

struct InMemList {
    InMemList(uint64_t numElements, uint64_t elementSize, bool requireNullMask)
        : numElements{numElements} {
        listData = std::make_unique<uint8_t[]>(numElements * elementSize);
        nullMask = requireNullMask ? std::make_unique<common::NullMask>(
                                         common::NullMask::getNumNullEntries(numElements)) :
                                     nullptr;
    }

    inline uint8_t* getListData() const { return listData.get(); }
    inline bool hasNullBuffer() const { return nullMask != nullptr; }
    inline uint64_t* getNullMask() const { return nullMask->getData(); }

    uint64_t numElements;
    std::unique_ptr<uint8_t[]> listData;
    std::unique_ptr<common::NullMask> nullMask;
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
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const common::DataType& dataType, const size_t& elementSize,
        std::shared_ptr<ListHeaders> headers, BufferManager& bufferManager, WAL* wal,
        ListsUpdatesStore* listsUpdatesStore)
        : Lists{storageStructureIDAndFName, dataType, elementSize, std::move(headers),
              bufferManager, true /*hasNULLBytes*/, wal, listsUpdatesStore} {};
    inline ListsMetadata& getListsMetadata() { return metadata; };
    inline std::shared_ptr<ListHeaders> getHeaders() const { return headers; };
    // TODO(Guodong): change the input to header.
    inline uint64_t getNumElementsFromListHeader(common::offset_t nodeOffset) const {
        auto header = headers->getHeader(nodeOffset);
        return ListHeaders::isALargeList(header) ?
                   metadata.getNumElementsInLargeLists(ListHeaders::getLargeListIdx(header)) :
                   ListHeaders::getSmallListLen(header);
    }
    inline uint64_t getNumElementsInListsUpdatesStore(common::offset_t nodeOffset) {
        return listsUpdatesStore->getNumInsertedRelsForNodeOffset(
            storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset);
    }
    inline uint64_t getTotalNumElementsInList(
        transaction::TransactionType transactionType, common::offset_t nodeOffset) {
        return getNumElementsInPersistentStore(transactionType, nodeOffset) +
               (transactionType == transaction::TransactionType::WRITE ?
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
    virtual inline void setDeletedRelsIfNecessary(transaction::Transaction* transaction,
        ListHandle& listHandle, common::ValueVector* valueVector) {
        // DO NOTHING.
    }
    virtual void readValues(transaction::Transaction* transaction, common::ValueVector* valueVector,
        ListHandle& listHandle);
    virtual void readFromSmallList(common::ValueVector* valueVector, ListHandle& listHandle);
    virtual void readFromLargeList(common::ValueVector* valueVector, ListHandle& listHandle);
    void readFromList(common::ValueVector* valueVector, ListHandle& listHandle);
    uint64_t getNumElementsInPersistentStore(
        transaction::TransactionType transactionType, common::offset_t nodeOffset);
    void initListReadingState(common::offset_t nodeOffset, ListHandle& listHandle,
        transaction::TransactionType transactionType);
    std::unique_ptr<InMemList> createInMemListWithDataFromUpdateStoreOnly(
        common::offset_t nodeOffset, std::vector<uint64_t>& insertedRelsTupleIdxInFT);
    // This function writes the persistent store data (skipping over the deleted rels) and update
    // store data to the inMemList.
    std::unique_ptr<InMemList> writeToInMemList(common::offset_t nodeOffset,
        const std::vector<uint64_t>& insertedRelTupleIdxesInFT,
        const std::unordered_set<uint64_t>& deletedRelOffsetsForList,
        UpdatedPersistentListOffsets* updatedPersistentListOffsets);
    void fillInMemListsFromPersistentStore(common::offset_t nodeOffset,
        uint64_t numElementsInPersistentStore, InMemList& inMemList,
        const std::unordered_set<list_offset_t>& deletedRelOffsetsInList,
        UpdatedPersistentListOffsets* updatedPersistentListOffsets = nullptr);

protected:
    virtual inline DiskOverflowFile* getDiskOverflowFileIfExists() { return nullptr; }
    Lists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const common::DataType& dataType, const size_t& elementSize,
        std::shared_ptr<ListHeaders> headers, BufferManager& bufferManager, bool hasNULLBytes,
        WAL* wal, ListsUpdatesStore* listsUpdatesStore)
        : BaseColumnOrList{storageStructureIDAndFName, dataType, elementSize, bufferManager,
              hasNULLBytes, wal},
          storageStructureIDAndFName{storageStructureIDAndFName},
          metadata{storageStructureIDAndFName, &bufferManager, wal}, headers{std::move(headers)},
          listsUpdatesStore{listsUpdatesStore} {};

private:
    void fillInMemListsFromFrame(InMemList& inMemList, const uint8_t* frame, uint64_t elemPosInPage,
        uint64_t numElementsToReadInCurPage,
        const std::unordered_set<uint64_t>& deletedRelOffsetsInList, uint64_t numElementsRead,
        uint64_t& nextPosToWriteToInMemList,
        UpdatedPersistentListOffsets* updatedPersistentListOffsets);

    void readPropertyUpdatesToInMemListIfExists(InMemList& inMemList,
        UpdatedPersistentListOffsets* updatedPersistentListOffsets, uint64_t numElementsRead,
        uint64_t numElementsToReadInCurPage, uint64_t nextPosToWriteToInMemList);

protected:
    StorageStructureIDAndFName storageStructureIDAndFName;
    ListsMetadata metadata;
    std::shared_ptr<ListHeaders> headers;
    ListsUpdatesStore* listsUpdatesStore;
};

class PropertyListsWithOverflow : public Lists {
public:
    PropertyListsWithOverflow(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const common::DataType& dataType, std::shared_ptr<ListHeaders> headers,
        BufferManager& bufferManager, WAL* wal, ListsUpdatesStore* listsUpdatesStore)
        : Lists{storageStructureIDAndFName, dataType, common::Types::getDataTypeSize(dataType),
              std::move(headers), bufferManager, wal, listsUpdatesStore},
          diskOverflowFile{storageStructureIDAndFName, bufferManager, wal} {}

private:
    inline DiskOverflowFile* getDiskOverflowFileIfExists() override { return &diskOverflowFile; }

public:
    DiskOverflowFile diskOverflowFile;
};

class StringPropertyLists : public PropertyListsWithOverflow {

public:
    StringPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const std::shared_ptr<ListHeaders>& headers, BufferManager& bufferManager, WAL* wal,
        ListsUpdatesStore* listsUpdatesStore)
        : PropertyListsWithOverflow{storageStructureIDAndFName, common::DataType{common::STRING},
              headers, bufferManager, wal, listsUpdatesStore} {};

private:
    void readFromLargeList(common::ValueVector* valueVector, ListHandle& listHandle) override;
    void readFromSmallList(common::ValueVector* valueVector, ListHandle& listHandle) override;
};

class ListPropertyLists : public PropertyListsWithOverflow {

public:
    ListPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const common::DataType& dataType, const std::shared_ptr<ListHeaders>& headers,
        BufferManager& bufferManager, WAL* wal, ListsUpdatesStore* listsUpdatesStore)
        : PropertyListsWithOverflow{storageStructureIDAndFName, dataType, headers, bufferManager,
              wal, listsUpdatesStore} {};

private:
    void readFromLargeList(common::ValueVector* valueVector, ListHandle& listHandle) override;
    void readFromSmallList(common::ValueVector* valueVector, ListHandle& listHandle) override;
};

class AdjLists : public Lists {

public:
    AdjLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        common::table_id_t nbrTableID, BufferManager& bufferManager, WAL* wal,
        ListsUpdatesStore* listsUpdatesStore)
        : Lists{storageStructureIDAndFName, common::DataType(common::INTERNAL_ID),
              sizeof(common::offset_t),
              std::make_shared<ListHeaders>(storageStructureIDAndFName, &bufferManager, wal),
              bufferManager, false /* hasNullBytes */, wal, listsUpdatesStore},
          nbrTableID{nbrTableID} {};

    inline bool mayContainNulls() const override { return false; }

    void readValues(transaction::Transaction* transaction, common::ValueVector* valueVector,
        ListHandle& listHandle) override;

    // Currently, used only in copyCSV tests.
    std::unique_ptr<std::vector<common::nodeID_t>> readAdjacencyListOfNode(
        common::offset_t nodeOffset);

    void checkpointInMemoryIfNecessary() override {
        headers->checkpointInMemoryIfNecessary();
        Lists::checkpointInMemoryIfNecessary();
    }

    void rollbackInMemoryIfNecessary() override {
        headers->rollbackInMemoryIfNecessary();
        Lists::rollbackInMemoryIfNecessary();
    }

private:
    void readFromLargeList(common::ValueVector* valueVector, ListHandle& listHandle) override;
    void readFromSmallList(common::ValueVector* valueVector, ListHandle& listHandle) override;
    void readFromListsUpdatesStore(ListHandle& listHandle, common::ValueVector* valueVector);
    void readFromPersistentStore(ListHandle& listHandle, common::ValueVector* valueVector);

private:
    common::table_id_t nbrTableID;
};

class RelIDList : public Lists {

public:
    RelIDList(const StorageStructureIDAndFName& storageStructureIDAndFName,
        std::shared_ptr<ListHeaders> headers, BufferManager& bufferManager, WAL* wal,
        ListsUpdatesStore* listsUpdatesStore)
        : Lists{storageStructureIDAndFName, common::DataType{common::INTERNAL_ID},
              sizeof(common::offset_t), std::move(headers), bufferManager, wal, listsUpdatesStore} {
    }

    void setDeletedRelsIfNecessary(transaction::Transaction* transaction, ListHandle& listHandle,
        common::ValueVector* relIDVector) override;

    std::unordered_set<uint64_t> getDeletedRelOffsetsInListForNodeOffset(
        common::offset_t nodeOffset);

    list_offset_t getListOffset(common::offset_t nodeOffset, common::offset_t relOffset);

    void readFromSmallList(common::ValueVector* valueVector, ListHandle& listHandle) override;

    void readFromLargeList(common::ValueVector* valueVector, ListHandle& listHandle) override;

private:
    inline bool mayContainNulls() const override { return false; }

    inline common::table_id_t getRelTableID() const {
        return storageStructureIDAndFName.storageStructureID.listFileID.relPropertyListID
            .relNodeTableAndDir.relTableID;
    }
};

class ListsFactory {

public:
    static std::unique_ptr<Lists> getLists(const StorageStructureIDAndFName& structureIDAndFName,
        const common::DataType& dataType, const std::shared_ptr<ListHeaders>& adjListsHeaders,
        BufferManager& bufferManager, WAL* wal, ListsUpdatesStore* listsUpdatesStore) {
        assert(listsUpdatesStore != nullptr);
        switch (dataType.typeID) {
        case common::INT64:
        case common::INT32:
        case common::INT16:
        case common::DOUBLE:
        case common::FLOAT:
        case common::BOOL:
        case common::DATE:
        case common::TIMESTAMP:
        case common::INTERVAL:
        case common::FIXED_LIST:
            return std::make_unique<Lists>(structureIDAndFName, dataType,
                common::Types::getDataTypeSize(dataType), adjListsHeaders, bufferManager, wal,
                listsUpdatesStore);
        case common::STRING:
            return std::make_unique<StringPropertyLists>(
                structureIDAndFName, adjListsHeaders, bufferManager, wal, listsUpdatesStore);
        case common::VAR_LIST:
            return std::make_unique<ListPropertyLists>(structureIDAndFName, dataType,
                adjListsHeaders, bufferManager, wal, listsUpdatesStore);
        case common::INTERNAL_ID:
            return std::make_unique<RelIDList>(
                structureIDAndFName, adjListsHeaders, bufferManager, wal, listsUpdatesStore);
        default:
            throw common::StorageException("Invalid type for property list creation.");
        }
    }
};

} // namespace storage
} // namespace kuzu
