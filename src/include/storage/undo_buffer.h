#pragma once

#include "common/constants.h"
#include "common/types/types.h"

namespace kuzu {
namespace catalog {
class CatalogEntry;
class CatalogSet;
class SequenceCatalogEntry;
struct SequenceData;
} // namespace catalog
namespace main {
class ClientContext;
}
namespace storage {

// TODO(Guodong): This should be reworked to use MemoryManager for memory allocaiton.
//                For now, we use malloc to get around the limitation of 256KB from MM.
class UndoMemoryBuffer {
public:
    static constexpr uint64_t UNDO_MEMORY_BUFFER_SIZE = common::BufferPoolConstants::PAGE_4KB_SIZE;

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

// This class is not thread safe, as it is supposed to be accessed by a single thread.
class UndoBuffer {
    friend class UndoBufferIterator;

public:
    enum class UndoRecordType : uint16_t {
        CATALOG_ENTRY,
        SEQUENCE_ENTRY,
    };

    explicit UndoBuffer(main::ClientContext& clientContext);

    void createCatalogEntry(catalog::CatalogSet& catalogSet, catalog::CatalogEntry& catalogEntry);
    void createSequenceChange(catalog::SequenceCatalogEntry& sequenceEntry,
        const catalog::SequenceData& data, int64_t prevVal);

    void commit(common::transaction_t commitTS) const;
    void rollback();

    UndoBufferIterator getIterator() const;

private:
    uint8_t* createUndoRecord(uint64_t size);

    void commitRecord(UndoRecordType recordType, uint8_t const* entry,
        common::transaction_t commitTS) const;
    void rollbackEntry(UndoRecordType recordType, uint8_t const* entry);

    void commitCatalogEntry(uint8_t const* entry, common::transaction_t commitTS) const;
    void commitSequenceEntry(uint8_t const* entry, common::transaction_t commitTS) const;
    void rollbackCatalogEntry(uint8_t const* entry);
    void rollbackSequenceEntry(uint8_t const* entry);

private:
    main::ClientContext& clientContext;
    std::vector<UndoMemoryBuffer> memoryBuffers;
};

} // namespace storage
} // namespace kuzu
