#include "storage/wal_replayer.h"

#include <unordered_map>

#include "binder/binder.h"
#include "catalog/catalog_entry/scalar_macro_catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "catalog/catalog_entry/type_catalog_entry.h"
#include "common/file_system/file_info.h"
#include "common/serializer/buffered_file.h"
#include "main/client_context.h"
#include "processor/expression_mapper.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/wal/wal_record.h"
#include "transaction/transaction.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

// COMMIT_CHECKPOINT:   isCheckpoint = true,  isRecovering = false
// ROLLBACK:            isCheckpoint = false, isRecovering = false
// RECOVERY_CHECKPOINT: isCheckpoint = true,  isRecovering = true
WALReplayer::WALReplayer(main::ClientContext& clientContext, WALReplayMode replayMode)
    : isRecovering{replayMode == WALReplayMode::RECOVERY_CHECKPOINT},
      isCheckpoint{replayMode != WALReplayMode::ROLLBACK}, clientContext{clientContext} {
    walFilePath = clientContext.getVFSUnsafe()->joinPath(clientContext.getDatabasePath(),
        StorageConstants::WAL_FILE_SUFFIX);
    pageBuffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
}

void WALReplayer::replay() {
    if (!clientContext.getVFSUnsafe()->fileOrPathExists(walFilePath, &clientContext)) {
        return;
    }
    auto fileInfo = clientContext.getVFSUnsafe()->openFile(walFilePath, O_RDONLY);
    auto walFileSize = fileInfo->getFileSize();
    // Check if the wal file is empty or corrupted. so nothing to read.
    if (walFileSize == 0) {
        return;
    }
    // TODO(Guodong): Handle the case that wal file is corrupted and there is no COMMIT record for
    // the last transaction.
    try {
        Deserializer deserializer(std::make_unique<BufferedFileReader>(std::move(fileInfo)));
        while (!deserializer.finished()) {
            auto walRecord = WALRecord::deserialize(deserializer);
            replayWALRecord(*walRecord);
        }
    } catch (const Exception& e) {
        throw RuntimeException(
            stringFormat("Failed to read wal record from WAL file. Error: {}", e.what()));
    }
}

void WALReplayer::replayWALRecord(WALRecord& walRecord) {
    switch (walRecord.type) {
    case WALRecordType::COMMIT_RECORD: {
    } break;
    case WALRecordType::CREATE_CATALOG_ENTRY_RECORD: {
        replayCreateCatalogEntryRecord(walRecord);
    } break;
    case WALRecordType::COPY_TABLE_RECORD: {
        replayCopyTableRecord(walRecord);
    } break;
    case WALRecordType::DROP_CATALOG_ENTRY_RECORD: {
        replayDropCatalogEntryRecord(walRecord);
    } break;
    case WALRecordType::ALTER_TABLE_ENTRY_RECORD: {
        replayAlterTableEntryRecord(walRecord);
    } break;
    case WALRecordType::UPDATE_SEQUENCE_RECORD: {
        replayUpdateSequenceRecord(walRecord);
    } break;
    case WALRecordType::CHECKPOINT_RECORD: {
        // This record should not be replayed. It is only used to indicate that the previous records
        // had been replayed and shadow files are created.
        KU_UNREACHABLE;
    }
    default:
        KU_UNREACHABLE;
    }
}

void WALReplayer::replayCreateCatalogEntryRecord(const WALRecord& walRecord) {
    if (!(isCheckpoint && isRecovering)) {
        // Nothing to do.
        return;
    }
    auto& createEntryRecord = walRecord.constCast<CreateCatalogEntryRecord>();
    switch (createEntryRecord.ownedCatalogEntry->getType()) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY:
    case CatalogEntryType::RDF_GRAPH_ENTRY: {
        auto& tableEntry = createEntryRecord.ownedCatalogEntry->constCast<TableCatalogEntry>();
        clientContext.getCatalog()->createTableSchema(&DUMMY_TRANSACTION,
            tableEntry.getBoundCreateTableInfo(&DUMMY_TRANSACTION));
    } break;
    case CatalogEntryType::SCALAR_MACRO_ENTRY: {
        auto& macroEntry =
            createEntryRecord.ownedCatalogEntry->constCast<ScalarMacroCatalogEntry>();
        clientContext.getCatalog()->addScalarMacroFunction(&DUMMY_TRANSACTION, macroEntry.getName(),
            macroEntry.getMacroFunction()->copy());
    } break;
    case CatalogEntryType::SEQUENCE_ENTRY: {
        auto& sequenceEntry =
            createEntryRecord.ownedCatalogEntry->constCast<SequenceCatalogEntry>();
        clientContext.getCatalog()->createSequence(&DUMMY_TRANSACTION,
            sequenceEntry.getBoundCreateSequenceInfo());
    } break;
    case CatalogEntryType::TYPE_ENTRY: {
        auto& typeEntry = createEntryRecord.ownedCatalogEntry->constCast<TypeCatalogEntry>();
        clientContext.getCatalog()->createType(&DUMMY_TRANSACTION, typeEntry.getName(),
            typeEntry.getLogicalType().copy());
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

// Replay catalog should only work under RECOVERY mode.
void WALReplayer::replayDropCatalogEntryRecord(const WALRecord& walRecord) {
    if (!(isCheckpoint && isRecovering)) {
        return;
    }
    auto& dropEntryRecord = walRecord.constCast<DropCatalogEntryRecord>();
    auto entryID = dropEntryRecord.entryID;
    switch (dropEntryRecord.entryType) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY:
    case CatalogEntryType::RDF_GRAPH_ENTRY: {
        clientContext.getCatalog()->dropTableEntry(&DUMMY_TRANSACTION, entryID);
        // During recovery, storageManager does not exist.
        if (clientContext.getStorageManager()) {
            clientContext.getStorageManager()->dropTable(entryID, clientContext.getVFSUnsafe());
        }
    } break;
    case CatalogEntryType::SEQUENCE_ENTRY: {
        clientContext.getCatalog()->dropSequence(&DUMMY_TRANSACTION, entryID);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void WALReplayer::replayAlterTableEntryRecord(const WALRecord& walRecord) {
    if (!(isCheckpoint && isRecovering)) {
        return;
    }
    auto binder = Binder(&clientContext);
    auto& alterEntryRecord = walRecord.constCast<AlterTableEntryRecord>();
    clientContext.getCatalog()->alterTableEntry(&DUMMY_TRANSACTION,
        *alterEntryRecord.ownedAlterInfo);
    if (alterEntryRecord.ownedAlterInfo->alterType == common::AlterType::ADD_PROPERTY) {
        auto exprBinder = binder.getExpressionBinder();
        auto addInfo =
            alterEntryRecord.ownedAlterInfo->extraInfo->constPtrCast<BoundExtraAddPropertyInfo>();
        // We don't implicit cast here since it must already be done the first time
        auto boundDefault = exprBinder->bindExpression(*addInfo->defaultValue);
        auto exprMapper = ExpressionMapper();
        auto defaultValueEvaluator = exprMapper.getEvaluator(boundDefault);
        defaultValueEvaluator->init(ResultSet(0) /* dummy ResultSet */, &clientContext);
        auto schema = clientContext.getCatalog()->getTableCatalogEntry(&DUMMY_TRANSACTION,
            alterEntryRecord.ownedAlterInfo->tableID);
        auto addedPropID = schema->getPropertyID(addInfo->propertyName);
        auto addedProp = schema->getProperty(addedPropID);
        TableAddColumnState state{*addedProp, *defaultValueEvaluator};
        if (clientContext.getStorageManager()) {
            auto storageManager = clientContext.getStorageManager();
            storageManager->getTable(alterEntryRecord.ownedAlterInfo->tableID)
                ->addColumn(&DUMMY_TRANSACTION, state);
        }
    }
}

void WALReplayer::replayCopyTableRecord(const WALRecord&) const {
    // DO NOTHING.
    // TODO(Guodong): Should handle metaDA and reclaim free pages when rollback.
}

void WALReplayer::replayUpdateSequenceRecord(const WALRecord& walRecord) {
    if (!(isCheckpoint && isRecovering)) {
        return;
    }
    auto& sequenceEntryRecord = walRecord.constCast<UpdateSequenceRecord>();
    auto sequenceID = sequenceEntryRecord.sequenceID;
    auto entry =
        clientContext.getCatalog()->getSequenceCatalogEntry(&DUMMY_TRANSACTION, sequenceID);
    entry->replayVal(sequenceEntryRecord.data.usageCount, sequenceEntryRecord.data.currVal,
        sequenceEntryRecord.data.nextVal);
}

} // namespace storage
} // namespace kuzu
