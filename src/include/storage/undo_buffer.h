#pragma once

#include <mutex>

#include "common/constants.h"
#include "common/types/types.h"

namespace kuzu {
namespace catalog {
class CatalogEntry;
class CatalogSet;
class SequenceCatalogEntry;
struct SequenceRollbackData;
} // namespace catalog
namespace transaction {
class Transaction;
}

namespace main {
class ClientContext;
}
namespace storage {

// TODO(Guodong): This should be reworked to use MemoryManager for memory allocaiton.
//                For now, we use malloc to get around the limitation of 256KB from MM.
class UndoMemoryBuffer {
public:
    static constexpr uint64_t UNDO_MEMORY_BUFFER_SIZE = common::KUZU_PAGE_SIZE;

    explicit UndoMemoryBuffer(uint64_t size) : size{size} {
        data = std::make_unique<uint8_t[]>(size);
        currentPosition = 0;
    }

    uint8_t* getDataUnsafe() const { return data.get(); }
    uint8_t const* getData() const { return data.get(); }
    uint64_t getSize() const { return size; }
    uint64_t getCurrentPosition() const { return currentPosition; }
    void moveCurrentPosition(uint64_t offset) {
        KU_ASSERT(currentPosition + offset <= size);
        currentPosition += offset;
    }
    bool canFit(uint64_t size_) const { return currentPosition + size_ <= this->size; }

private:
    std::unique_ptr<uint8_t[]> data;
    uint64_t size;
    uint64_t currentPosition;
};

class UndoBuffer;
class UndoBufferIterator {
public:
    explicit UndoBufferIterator(const UndoBuffer& undoBuffer) : undoBuffer{undoBuffer} {}

    template<typename F>
    void iterate(F&& callback);
    template<typename F>
    void reverseIterate(F&& callback);

private:
    const UndoBuffer& undoBuffer;
};

class UpdateInfo;
class VersionInfo;
struct VectorUpdateInfo;
class ChunkedNodeGroup;
class WAL;
// This class is not thread safe, as it is supposed to be accessed by a single thread.
class UndoBuffer {
    friend class UndoBufferIterator;

public:
    enum class UndoRecordType : uint16_t {
        CATALOG_ENTRY = 0,
        SEQUENCE_ENTRY = 1,
        UPDATE_INFO = 6,
        INSERT_INFO = 7,
        DELETE_INFO = 8,
    };

    explicit UndoBuffer(transaction::Transaction* transaction);

    void createCatalogEntry(catalog::CatalogSet& catalogSet, catalog::CatalogEntry& catalogEntry);
    void createSequenceChange(catalog::SequenceCatalogEntry& sequenceEntry,
        const catalog::SequenceRollbackData& data);
    void createInsertInfo(ChunkedNodeGroup* chunkedNodeGroup, common::row_idx_t startRow,
        common::row_idx_t numRows);
    void createDeleteInfo(ChunkedNodeGroup* chunkedNodeGroup, common::row_idx_t startRow,
        common::row_idx_t numRows);
    void createVectorUpdateInfo(UpdateInfo* updateInfo, common::idx_t vectorIdx,
        VectorUpdateInfo* vectorUpdateInfo);

    void commit(common::transaction_t commitTS) const;
    void rollback();

    uint64_t getMemUsage() const;

private:
    uint8_t* createUndoRecord(uint64_t size);

    void createVersionInfo(UndoRecordType recordType, ChunkedNodeGroup* chunkedNodeGroup,
        common::row_idx_t startRow, common::row_idx_t numRows);

    void commitRecord(UndoRecordType recordType, const uint8_t* record,
        common::transaction_t commitTS) const;
    void rollbackRecord(UndoRecordType recordType, const uint8_t* record);

    void commitCatalogEntryRecord(const uint8_t* record, common::transaction_t commitTS) const;
    void rollbackCatalogEntryRecord(const uint8_t* record);

    void commitSequenceEntry(uint8_t const* entry, common::transaction_t commitTS) const;
    void rollbackSequenceEntry(uint8_t const* entry);

    void commitVersionInfo(UndoRecordType recordType, const uint8_t* record,
        common::transaction_t commitTS) const;
    void rollbackVersionInfo(UndoRecordType recordType, const uint8_t* record);

    void commitVectorUpdateInfo(const uint8_t* record, common::transaction_t commitTS) const;
    void rollbackVectorUpdateInfo(const uint8_t* record) const;

private:
    std::mutex mtx;
    transaction::Transaction* transaction;
    std::vector<UndoMemoryBuffer> memoryBuffers;
};

} // namespace storage
} // namespace kuzu
