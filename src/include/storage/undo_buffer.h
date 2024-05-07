#pragma once

#include "common/constants.h"
#include "common/types/types.h"

namespace kuzu {
namespace catalog {
class CatalogEntry;
class CatalogSet;
} // namespace catalog
namespace main {
class ClientContext;
}
namespace storage {

// TODO(Guodong): This should be reworked to use MemoryManager for memory allocaiton.
//                For now, we use malloc to get around the limitation of 256KB from MM.
class UndoMemoryBuffer {
public:
    static constexpr const uint64_t UNDO_MEMORY_BUFFER_SIZE =
        common::BufferPoolConstants::PAGE_4KB_SIZE;

    explicit UndoMemoryBuffer(uint64_t size) : size{size} {
        data = std::make_unique<uint8_t[]>(size);
        currentPosition = 0;
    }

    uint8_t* getDataUnsafe() { return data.get(); }
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
    enum class UndoEntryType : uint16_t {
        CATALOG_ENTRY,
    };

    explicit UndoBuffer(main::ClientContext& clientContext);

    void createCatalogEntry(catalog::CatalogSet& catalogSet, catalog::CatalogEntry& catalogEntry);

    void commit(common::transaction_t commitTS);
    void rollback();

    UndoBufferIterator getIterator() const;

private:
    uint8_t* createUndoEntry(uint64_t size);

    void commitEntry(uint8_t const* entry, common::transaction_t commitTS);
    void rollbackEntry(uint8_t const* entry);

private:
    main::ClientContext& clientContext;
    std::vector<UndoMemoryBuffer> memoryBuffers;
};

} // namespace storage
} // namespace kuzu
