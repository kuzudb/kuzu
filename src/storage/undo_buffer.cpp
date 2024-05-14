#include "storage/undo_buffer.h"

#include "catalog/catalog_entry/catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace storage {

template<typename F>
void UndoBufferIterator::iterate(F&& callback) {
    uint8_t const* end;
    common::vector_idx_t bufferIdx = 0;
    while (bufferIdx < undoBuffer.memoryBuffers.size()) {
        auto& currentBuffer = undoBuffer.memoryBuffers[bufferIdx];
        auto current = currentBuffer.getData();
        end = current + currentBuffer.getCurrentPosition();
        while (current < end) {
            UndoBuffer::UndoEntryType entryType =
                *reinterpret_cast<UndoBuffer::UndoEntryType const*>(current);
            // Only support catalog for now.
            KU_ASSERT(entryType == UndoBuffer::UndoEntryType::CATALOG_ENTRY);
            current += sizeof(UndoBuffer::UndoEntryType);
            auto entrySize = *reinterpret_cast<uint32_t const*>(current);
            current += sizeof(uint32_t); // Skip entrySize field.
            callback(current);
            current += entrySize; // Skip the current entry.
        }
        bufferIdx++;
    }
}

template<typename F>
void UndoBufferIterator::reverseIterate(F&& callback) {
    uint8_t const* end;
    common::vector_idx_t numBuffersLeft = undoBuffer.memoryBuffers.size();
    while (numBuffersLeft > 0) {
        auto bufferIdx = numBuffersLeft - 1;
        auto& currentBuffer = undoBuffer.memoryBuffers[bufferIdx];
        auto current = currentBuffer.getData();
        end = current + currentBuffer.getCurrentPosition();
        std::vector<uint8_t const*> entries;
        while (current < end) {
            UndoBuffer::UndoEntryType entryType =
                *reinterpret_cast<UndoBuffer::UndoEntryType const*>(current);
            // Only support catalog for now.
            KU_ASSERT(entryType == UndoBuffer::UndoEntryType::CATALOG_ENTRY);
            current += sizeof(UndoBuffer::UndoEntryType);
            auto entrySize = *reinterpret_cast<uint32_t const*>(current);
            current += sizeof(uint32_t); // Skip entrySize field.
            entries.push_back(current);
            current += entrySize; // Skip the current entry.
        }
        for (auto i = entries.size(); i >= 1; i--) {
            callback(entries[i - 1]);
        }
        numBuffersLeft--;
    }
}

UndoBuffer::UndoBuffer(ClientContext& clientContext) : clientContext{clientContext} {}

void UndoBuffer::createCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry) {
    // We store an pointer to catalog entry inside the undo buffer.
    // Each catalog undo entry has the following format:
    //      [entryType: UndoEntryType][entrySize: uint32_t][pointer: CatalogEntry*][ponter:
    //      CatalogSet*]
    auto buffer = createUndoEntry(
        sizeof(UndoEntryType) + sizeof(uint32_t) + sizeof(CatalogEntry*) + sizeof(CatalogSet*));
    *reinterpret_cast<UndoEntryType*>(buffer) = UndoEntryType::CATALOG_ENTRY;
    buffer += sizeof(UndoEntryType);
    *reinterpret_cast<uint32_t*>(buffer) = sizeof(CatalogEntry*) + sizeof(CatalogSet*);
    buffer += sizeof(uint32_t);
    *reinterpret_cast<CatalogEntry**>(buffer) = &catalogEntry;
    buffer += sizeof(CatalogEntry*);
    *reinterpret_cast<CatalogSet**>(buffer) = &catalogSet;
}

uint8_t* UndoBuffer::createUndoEntry(uint64_t size) {
    if (memoryBuffers.empty() || !memoryBuffers.back().canFit(size)) {
        auto capacity = UndoMemoryBuffer::UNDO_MEMORY_BUFFER_SIZE;
        while (size > capacity) {
            capacity *= 2;
        }
        // We need to allocate a new memory buffer.
        memoryBuffers.push_back(UndoMemoryBuffer(capacity));
    }
    auto res = memoryBuffers.back().getDataUnsafe() + memoryBuffers.back().getCurrentPosition();
    memoryBuffers.back().moveCurrentPosition(size);
    return res;
}

void UndoBuffer::commit(transaction_t commitTS) {
    UndoBufferIterator iterator{*this};
    iterator.iterate([&](uint8_t const* entry) { commitEntry(entry, commitTS); });
}

void UndoBuffer::rollback() {
    UndoBufferIterator iterator{*this};
    iterator.reverseIterate([&](uint8_t const* entry) { rollbackEntry(entry); });
}

void UndoBuffer::commitEntry(const uint8_t* entry, transaction_t commitTS) {
    auto& catalogEntry = *reinterpret_cast<CatalogEntry* const*>(entry);
    auto newCatalogEntry = catalogEntry->getNext();
    KU_ASSERT(newCatalogEntry);
    newCatalogEntry->setTimestamp(commitTS);
    auto& wal = clientContext.getStorageManager()->getWAL();
    switch (newCatalogEntry->getType()) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY:
    case CatalogEntryType::RDF_GRAPH_ENTRY:
    case CatalogEntryType::SCALAR_MACRO_ENTRY: {
        if (catalogEntry->getType() == CatalogEntryType::DUMMY_ENTRY) {
            KU_ASSERT(catalogEntry->isDeleted());
            wal.logCreateCatalogEntryRecord(newCatalogEntry);
        }
    } break;
    case CatalogEntryType::SCALAR_FUNCTION_ENTRY: {
        // DO NOTHING. We don't persistent function entries.
    } break;
    case CatalogEntryType::SEQUENCE_ENTRY: {
        if (catalogEntry->getType() == CatalogEntryType::DUMMY_ENTRY) {
            KU_ASSERT(catalogEntry->isDeleted());
            wal.logCreateSequenceRecord(newCatalogEntry);
        }
    } break;
    case CatalogEntryType::DUMMY_ENTRY: {
        KU_ASSERT(newCatalogEntry->isDeleted());
        switch (catalogEntry->getType()) {
        case CatalogEntryType::NODE_TABLE_ENTRY:
        case CatalogEntryType::REL_TABLE_ENTRY:
        case CatalogEntryType::REL_GROUP_ENTRY:
        case CatalogEntryType::RDF_GRAPH_ENTRY: {
            auto tableCatalogEntry =
                ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(catalogEntry);
            wal.logDropTableRecord(tableCatalogEntry->getTableID(), tableCatalogEntry->getType());
        } break;
        case CatalogEntryType::SEQUENCE_ENTRY: {
            auto sequenceCatalogEntry =
                ku_dynamic_cast<CatalogEntry*, SequenceCatalogEntry*>(catalogEntry);
            wal.logDropSequenceRecord(sequenceCatalogEntry->getSequenceID());
        } break;
        default: {
            throw RuntimeException(stringFormat("Not supported catalog entry type {} yet.",
                CatalogEntryTypeUtils::toString(catalogEntry->getType())));
        }
        }
    } break;
    default: {
        throw RuntimeException(stringFormat("Not supported catalog entry type {} yet.",
            CatalogEntryTypeUtils::toString(catalogEntry->getType())));
    }
    }
}

void UndoBuffer::rollbackEntry(const uint8_t* entry) {
    auto& catalogEntry = *reinterpret_cast<CatalogEntry* const*>(entry);
    auto& catalogSet = *reinterpret_cast<CatalogSet* const*>(entry + sizeof(CatalogEntry*));
    auto entryToRollback = catalogEntry->getNext();
    KU_ASSERT(entryToRollback);
    if (entryToRollback->getNext()) {
        // If entryToRollback has a newer entry (next) in the version chain. Simple remove
        // entryToRollback from the chain.
        auto newerEntry = entryToRollback->getNext();
        newerEntry->setPrev(entryToRollback->movePrev());
    } else {
        // This is the begin of the version chain.
        auto olderEntry = entryToRollback->movePrev();
        catalogSet->erase(catalogEntry->getName());
        if (olderEntry) {
            catalogSet->emplace(std::move(olderEntry));
        }
    }
}

} // namespace storage
} // namespace kuzu
