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

struct UndoRecordHeader {
    UndoBuffer::UndoRecordType recordType;
    uint32_t recordSize;

    UndoRecordHeader(const UndoBuffer::UndoRecordType recordType, const uint32_t recordSize)
        : recordType{recordType}, recordSize{recordSize} {}
};

struct CatalogEntryRecord {
    CatalogSet* catalogSet;
    CatalogEntry* catalogEntry;
};

struct VectorVersionRecord {
    NodeGroupVersionInfo* versionInfo;
    idx_t vectorIdx;
    VectorVersionInfo* vectorVersionInfo;
    std::vector<row_idx_t> rowsInVector;
};

struct VectorUpdateRecord {
    UpdateInfo* updateInfo;
    idx_t vectorIdx;
    VectorUpdateInfo* vectorUpdateInfo;
};

template<typename F>
void UndoBufferIterator::iterate(F&& callback) {
    idx_t bufferIdx = 0;
    while (bufferIdx < undoBuffer.memoryBuffers.size()) {
        auto& currentBuffer = undoBuffer.memoryBuffers[bufferIdx];
        auto current = currentBuffer.getData();
        uint8_t const* end = current + currentBuffer.getCurrentPosition();
        while (current < end) {
            UndoRecordHeader recordHeader = *reinterpret_cast<UndoRecordHeader const*>(current);
            current += sizeof(UndoRecordHeader);
            callback(recordHeader.recordType, current);
            current += recordHeader.recordSize; // Skip the current entry.
        }
        bufferIdx++;
    }
}

template<typename F>
void UndoBufferIterator::reverseIterate(F&& callback) {
    idx_t numBuffersLeft = undoBuffer.memoryBuffers.size();
    while (numBuffersLeft > 0) {
        const auto bufferIdx = numBuffersLeft - 1;
        auto& currentBuffer = undoBuffer.memoryBuffers[bufferIdx];
        auto current = currentBuffer.getData();
        uint8_t const* end = current + currentBuffer.getCurrentPosition();
        std::vector<std::pair<UndoBuffer::UndoRecordType, uint8_t const*>> entries;
        while (current < end) {
            UndoRecordHeader recordHeader = *reinterpret_cast<UndoRecordHeader const*>(current);
            current += sizeof(UndoRecordHeader);
            entries.push_back({recordHeader.recordType, current});
            current += sizeof(recordHeader.recordSize); // Skip the current entry.
        }
        for (auto i = entries.size(); i >= 1; i--) {
            callback(entries[i - 1].first, entries[i - 1].second);
        }
        numBuffersLeft--;
    }
}

UndoBuffer::UndoBuffer(ClientContext& clientContext) : clientContext{clientContext} {}

void UndoBuffer::createCatalogEntry(CatalogSet* catalogSet, CatalogEntry* catalogEntry) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(CatalogEntryRecord));
    const UndoRecordHeader recorHeader{UndoRecordType::CATALOG_ENTRY, sizeof(CatalogEntryRecord)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recorHeader;
    buffer += sizeof(UndoRecordHeader);
    const CatalogEntryRecord catalogEntryRecord{catalogSet, catalogEntry};
    *reinterpret_cast<CatalogEntryRecord*>(buffer) = catalogEntryRecord;
}

void UndoBuffer::createVectorInsertInfo(NodeGroupVersionInfo* versionInfo, const idx_t vectorIdx,
    VectorVersionInfo* vectorVersionInfo, const std::vector<row_idx_t>& rowsInVector) {
    createVectorVersionInfo(UndoRecordType::INSERT_INFO, versionInfo, vectorIdx, vectorVersionInfo,
        rowsInVector);
}

void UndoBuffer::createVectorDeleteInfo(NodeGroupVersionInfo* versionInfo, const idx_t vectorIdx,
    VectorVersionInfo* vectorVersionInfo, const std::vector<row_idx_t>& rowsInVector) {
    createVectorVersionInfo(UndoRecordType::DELETE_INFO, versionInfo, vectorIdx, vectorVersionInfo,
        rowsInVector);
}

void UndoBuffer::createVectorVersionInfo(const UndoRecordType recordType,
    NodeGroupVersionInfo* versionInfo, const idx_t vectorIdx, VectorVersionInfo* vectorVersionInfo,
    const std::vector<row_idx_t>& rowsInVector) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(VectorVersionRecord));
    const UndoRecordHeader recordHeader{recordType, sizeof(VectorVersionRecord)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recordHeader;
    buffer += sizeof(UndoRecordHeader);
    const VectorVersionRecord vectorVersionRecord{versionInfo, vectorIdx, vectorVersionInfo,
        rowsInVector};
    *reinterpret_cast<VectorVersionRecord*>(buffer) = vectorVersionRecord;
}

void UndoBuffer::createVectorUpdateInfo(UpdateInfo* updateInfo, const idx_t vectorIdx,
    VectorUpdateInfo* vectorUpdateInfo) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(VectorUpdateInfo));
    const UndoRecordHeader recorHeader{UndoRecordType::UPDATE_INFO, sizeof(VectorUpdateInfo)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recorHeader;
    buffer += sizeof(UndoRecordHeader);
    const VectorUpdateRecord vectorUpdateRecord{updateInfo, vectorIdx, vectorUpdateInfo};
    *reinterpret_cast<VectorUpdateRecord*>(buffer) = vectorUpdateRecord;
}

uint8_t* UndoBuffer::createUndoRecord(const uint64_t size) {
    if (memoryBuffers.empty() || !memoryBuffers.back().canFit(size)) {
        auto capacity = UndoMemoryBuffer::UNDO_MEMORY_BUFFER_SIZE;
        while (size > capacity) {
            capacity *= 2;
        }
        // We need to allocate a new memory buffer.
        memoryBuffers.push_back(UndoMemoryBuffer(capacity));
    }
    const auto res =
        memoryBuffers.back().getDataUnsafe() + memoryBuffers.back().getCurrentPosition();
    memoryBuffers.back().moveCurrentPosition(size);
    return res;
}

void UndoBuffer::commit(const transaction_t commitTS) const {
    UndoBufferIterator iterator{*this};
    iterator.iterate([&](const UndoRecordType recordType, uint8_t const* entry) {
        commitRecord(recordType, entry, commitTS);
    });
}

void UndoBuffer::rollback() {
    UndoBufferIterator iterator{*this};
    iterator.reverseIterate([&](const UndoRecordType recordType, uint8_t const* entry) {
        rollbackRecord(recordType, entry);
    });
}

void UndoBuffer::commitRecord(const UndoRecordType recordType, const uint8_t* record,
    const transaction_t commitTS) const {
    switch (recordType) {
    case UndoRecordType::CATALOG_ENTRY: {
        commitCatalogEntryRecord(record, commitTS);
    } break;
    case UndoRecordType::INSERT_INFO:
    case UndoRecordType::DELETE_INFO: {
        commitVectorVersionInfo(recordType, record, commitTS);
    } break;
    case UndoRecordType::UPDATE_INFO: {
        commitVectorUpdateInfo(record, commitTS);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void UndoBuffer::commitCatalogEntryRecord(const uint8_t* record,
    const transaction_t commitTS) const {
    const auto& [_, catalogEntry] = *reinterpret_cast<CatalogEntryRecord const*>(record);
    const auto newCatalogEntry = catalogEntry->getNext();
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
    case CatalogEntryType::TYPE_ENTRY: {
        if (catalogEntry->getType() == CatalogEntryType::DUMMY_ENTRY) {
            KU_ASSERT(catalogEntry->isDeleted());
            wal.logCreateTypeRecord(newCatalogEntry);
        }
    } break;
    case CatalogEntryType::DUMMY_ENTRY: {
        KU_ASSERT(newCatalogEntry->isDeleted());
        switch (catalogEntry->getType()) {
        case CatalogEntryType::NODE_TABLE_ENTRY:
        case CatalogEntryType::REL_TABLE_ENTRY:
        case CatalogEntryType::REL_GROUP_ENTRY:
        case CatalogEntryType::RDF_GRAPH_ENTRY:
            // TODO: Add support for dropping macro.
        case CatalogEntryType::SCALAR_MACRO_ENTRY: {
            const auto& tableCatalogEntry = catalogEntry->constCast<TableCatalogEntry>();
            wal.logDropTableRecord(tableCatalogEntry.getTableID(), tableCatalogEntry.getType());
        } break;
        case CatalogEntryType::SEQUENCE_ENTRY: {
            const auto& sequenceCatalogEntry = catalogEntry->constCast<SequenceCatalogEntry>();
            wal.logDropSequenceRecord(sequenceCatalogEntry.getSequenceID());
        } break;
        case CatalogEntryType::SCALAR_FUNCTION_ENTRY: {
            // DO NOTHING. We don't persistent function entries.
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

void UndoBuffer::commitVectorVersionInfo(UndoRecordType recordType, const uint8_t* record,
    transaction_t commitTS) const {
    const auto& undoRecord = *reinterpret_cast<VectorVersionRecord const*>(record);
    switch (recordType) {
    case UndoRecordType::INSERT_INFO: {
        for (const auto rowIdx : undoRecord.rowsInVector) {
            undoRecord.vectorVersionInfo->insertedVersions[rowIdx] = commitTS;
        }
    } break;
    case UndoRecordType::DELETE_INFO: {
        for (const auto rowIdx : undoRecord.rowsInVector) {
            undoRecord.vectorVersionInfo->deletedVersions[rowIdx] = commitTS;
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void UndoBuffer::commitVectorUpdateInfo(const uint8_t* record, transaction_t commitTS) const {
    auto& undoRecord = *reinterpret_cast<VectorUpdateRecord const*>(record);
    undoRecord.vectorUpdateInfo->version = commitTS;
}

void UndoBuffer::rollbackRecord(const UndoRecordType recordType, const uint8_t* record) {
    switch (recordType) {
    case UndoRecordType::CATALOG_ENTRY: {
        rollbackCatalogEntryRecord(record);
    } break;
    case UndoRecordType::INSERT_INFO:
    case UndoRecordType::DELETE_INFO: {
        rollbackVectorVersionInfo(recordType, record);
    } break;
    case UndoRecordType::UPDATE_INFO: {
        rollbackVectorUpdateInfo(record);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void UndoBuffer::rollbackCatalogEntryRecord(const uint8_t* record) {
    const auto& [catalogSet, catalogEntry] = *reinterpret_cast<CatalogEntryRecord const*>(record);
    const auto entryToRollback = catalogEntry->getNext();
    KU_ASSERT(entryToRollback);
    if (entryToRollback->getNext()) {
        // If entryToRollback has a newer entry (next) in the version chain. Simple remove
        // entryToRollback from the chain.
        const auto newerEntry = entryToRollback->getNext();
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

void UndoBuffer::rollbackVectorVersionInfo(UndoRecordType recordType, const uint8_t* record) {
    auto& undoRecord = *reinterpret_cast<VectorVersionRecord const*>(record);
    switch (recordType) {
    case UndoRecordType::INSERT_INFO: {
        for (const auto row : undoRecord.rowsInVector) {
            undoRecord.vectorVersionInfo->insertedVersions[row] = INVALID_TRANSACTION;
        }
        bool hasAnyInsertions = false;
        for (const auto& version : undoRecord.vectorVersionInfo->insertedVersions) {
            if (version != INVALID_TRANSACTION) {
                hasAnyInsertions = true;
                break;
            }
        }
        if (!hasAnyInsertions) {
            if (!undoRecord.vectorVersionInfo->anyDeleted) {
                undoRecord.versionInfo->clearVectorInfo(undoRecord.vectorIdx);
            } else {
                undoRecord.vectorVersionInfo->anyInserted = false;
            }
        }
    } break;
    case UndoRecordType::DELETE_INFO: {
        for (const auto row : undoRecord.rowsInVector) {
            undoRecord.vectorVersionInfo->deletedVersions[row] = INVALID_TRANSACTION;
        }
        bool hasAnyDeletions = false;
        for (const auto& version : undoRecord.vectorVersionInfo->deletedVersions) {
            if (version != INVALID_TRANSACTION) {
                hasAnyDeletions = true;
                break;
            }
        }
        if (!hasAnyDeletions) {
            if (!undoRecord.vectorVersionInfo->anyInserted) {
                undoRecord.versionInfo->clearVectorInfo(undoRecord.vectorIdx);
            } else {
                undoRecord.vectorVersionInfo->anyDeleted = false;
            }
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void UndoBuffer::rollbackVectorUpdateInfo(const uint8_t* record) {
    auto& undoRecord = *reinterpret_cast<VectorUpdateRecord const*>(record);
    KU_ASSERT(undoRecord.updateInfo);
    if (undoRecord.vectorUpdateInfo->getNext()) {
        // Has newer versions. Simply remove the current one from the version chain.
        const auto newerVersion = undoRecord.vectorUpdateInfo->getNext();
        auto prevVersion = undoRecord.vectorUpdateInfo->movePrev();
        prevVersion->next = newerVersion;
        newerVersion->setPrev(std::move(prevVersion));
    } else {
        // This is the begin of the version chain.
        if (auto prevVersion = undoRecord.vectorUpdateInfo->movePrev()) {
            undoRecord.updateInfo->setVectorInfo(undoRecord.vectorIdx, std::move(prevVersion));
        } else {
            undoRecord.updateInfo->clearVectorInfo(undoRecord.vectorIdx);
        }
    }
}

} // namespace storage
} // namespace kuzu
