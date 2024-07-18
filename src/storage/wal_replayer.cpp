#include "storage/wal_replayer.h"

#include "binder/binder.h"
#include "catalog/catalog_entry/scalar_macro_catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "catalog/catalog_entry/type_catalog_entry.h"
#include "common/file_system/file_info.h"
#include "common/serializer/buffered_file.h"
#include "main/client_context.h"
#include "processor/expression_mapper.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/store/node_table.h"
#include "storage/wal/wal_record.h"
#include "transaction/transaction_manager.h"

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
    const auto walFileSize = fileInfo->getFileSize();
    // Check if the wal file is empty or corrupted. so nothing to read.
    if (walFileSize == 0) {
        return;
    }
    // TODO(Guodong): Handle the case that wal file is corrupted and there is no COMMIT record for
    // the last transaction.
    try {
        Deserializer deserializer(std::make_unique<BufferedFileReader>(std::move(fileInfo)));
        while (!deserializer.finished()) {
            auto walRecord = WALRecord::deserialize(deserializer, clientContext);
            clientContext.getTransactionContext()->beginRecoveryTransaction();
            replayWALRecord(*walRecord);
            clientContext.getTransactionContext()->commit();
        }
    } catch (const Exception& e) {
        throw RuntimeException(
            stringFormat("Failed to read wal record from WAL file. Error: {}", e.what()));
    }
}

void WALReplayer::replayWALRecord(const WALRecord& walRecord) {
    switch (walRecord.type) {
    case WALRecordType::BEGIN_TRANSACTION_RECORD: {
        // clientContext.getTransactionContext()->beginRecoveryTransaction();
    } break;
    case WALRecordType::COMMIT_RECORD: {
        // clientContext.getTransactionContext()->commit();
    } break;
    case WALRecordType::CREATE_CATALOG_ENTRY_RECORD: {
        replayCreateCatalogEntryRecord(walRecord);
    } break;
    case WALRecordType::TABLE_INSERTION_RECORD: {
        replayTableInsertionRecord(walRecord);
    } break;
    case WALRecordType::NODE_DELETION_RECORD: {
        replayNodeDeletionRecord(walRecord);
    } break;
    case WALRecordType::NODE_UDPATE_RECORD: {
        replayNodeUpdateRecord(walRecord);
    } break;
    case WALRecordType::REL_DELETION_RECORD: {
        replayRelDeletionRecord(walRecord);
    } break;
    case WALRecordType::REL_UPDATE_RECORD: {
        replayRelUpdateRecord(walRecord);
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

void WALReplayer::replayCreateCatalogEntryRecord(const WALRecord& walRecord) const {
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
void WALReplayer::replayDropCatalogEntryRecord(const WALRecord& walRecord) const {
    if (!(isCheckpoint && isRecovering)) {
        return;
    }
    auto& dropEntryRecord = walRecord.constCast<DropCatalogEntryRecord>();
    const auto entryID = dropEntryRecord.entryID;
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

void WALReplayer::replayAlterTableEntryRecord(const WALRecord& walRecord) const {
    if (!(isCheckpoint && isRecovering)) {
        return;
    }
    auto binder = Binder(&clientContext);
    auto& alterEntryRecord = walRecord.constCast<AlterTableEntryRecord>();
    clientContext.getCatalog()->alterTableEntry(&DUMMY_TRANSACTION,
        *alterEntryRecord.ownedAlterInfo);
    if (alterEntryRecord.ownedAlterInfo->alterType == common::AlterType::ADD_PROPERTY) {
        const auto exprBinder = binder.getExpressionBinder();
        const auto addInfo =
            alterEntryRecord.ownedAlterInfo->extraInfo->constPtrCast<BoundExtraAddPropertyInfo>();
        // We don't implicit cast here since it must already be done the first time
        const auto boundDefault = exprBinder->bindExpression(*addInfo->defaultValue);
        auto exprMapper = ExpressionMapper();
        const auto defaultValueEvaluator = exprMapper.getEvaluator(boundDefault);
        const auto schema = clientContext.getCatalog()->getTableCatalogEntry(&DUMMY_TRANSACTION,
            alterEntryRecord.ownedAlterInfo->tableID);
        const auto addedPropID = schema->getPropertyID(addInfo->propertyName);
        const auto addedProp = schema->getProperty(addedPropID);
        TableAddColumnState state{*addedProp, *defaultValueEvaluator};
        if (clientContext.getStorageManager()) {
            const auto storageManager = clientContext.getStorageManager();
            storageManager->getTable(alterEntryRecord.ownedAlterInfo->tableID)
                ->addColumn(&DUMMY_TRANSACTION, state);
        }
    }
}

void WALReplayer::replayTableInsertionRecord(const WALRecord& walRecord) const {
    const auto& insertionRecord = walRecord.constCast<TableInsertionRecord>();
    switch (insertionRecord.tableType) {
    case TableType::NODE: {
        replayNodeTableInsertRecord(walRecord);
    } break;
    case TableType::REL: {
        replayRelTableInsertRecord(walRecord);
    } break;
    default: {
        throw RuntimeException("Invalid table type for insertion replay in WAL record.");
    }
    }
}

void WALReplayer::replayNodeTableInsertRecord(const WALRecord& walRecord) const {
    const auto& insertionRecord = walRecord.constCast<TableInsertionRecord>();
    const auto tableID = insertionRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<NodeTable>();
    const auto chunkState = std::make_shared<DataChunkState>();
    chunkState->getSelVectorUnsafe().setSelSize(insertionRecord.numRows);
    for (auto i = 0u; i < insertionRecord.ownedVectors.size(); i++) {
        insertionRecord.ownedVectors[i]->setState(chunkState);
    }
    std::vector<ValueVector*> propertyVectors(insertionRecord.ownedVectors.size());
    for (auto i = 0u; i < insertionRecord.ownedVectors.size(); i++) {
        propertyVectors[i] = insertionRecord.ownedVectors[i].get();
    }
    KU_ASSERT(table.getPKColumnID() < insertionRecord.ownedVectors.size());
    auto& pkVector = *insertionRecord.ownedVectors[table.getPKColumnID()];
    const auto nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
    nodeIDVector->setState(chunkState);
    const auto insertState =
        std::make_unique<NodeTableInsertState>(*nodeIDVector, pkVector, propertyVectors);
    KU_ASSERT(clientContext.getTx() && clientContext.getTx()->isRecovery());
    table.insert(clientContext.getTx(), *insertState);
}

void WALReplayer::replayRelTableInsertRecord(const WALRecord& walRecord) const {
    const auto& insertionRecord = walRecord.constCast<TableInsertionRecord>();
    const auto tableID = insertionRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<RelTable>();
    const auto chunkState = std::make_shared<DataChunkState>();
    chunkState->getSelVectorUnsafe().setSelSize(insertionRecord.numRows);
    for (auto i = 0u; i < insertionRecord.ownedVectors.size(); i++) {
        insertionRecord.ownedVectors[i]->setState(chunkState);
    }
    std::vector<ValueVector*> propertyVectors;
    for (auto i = 0u; i < insertionRecord.ownedVectors.size(); i++) {
        if (i < LOCAL_REL_ID_COLUMN_ID) {
            // Skip the first two vectors which are the src nodeID and the dst nodeID.
            continue;
        }
        propertyVectors.push_back(insertionRecord.ownedVectors[i].get());
    }
    const auto insertState = std::make_unique<RelTableInsertState>(
        *insertionRecord.ownedVectors[LOCAL_BOUND_NODE_ID_COLUMN_ID],
        *insertionRecord.ownedVectors[LOCAL_NBR_NODE_ID_COLUMN_ID], propertyVectors);
    KU_ASSERT(clientContext.getTx() && clientContext.getTx()->isRecovery());
    table.insert(clientContext.getTx(), *insertState);
}

void WALReplayer::replayNodeDeletionRecord(const WALRecord& walRecord) const {
    const auto& deletionRecord = walRecord.constCast<NodeDeletionRecord>();
    const auto tableID = deletionRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<NodeTable>();
    const auto chunkState = std::make_shared<DataChunkState>();
    chunkState->getSelVectorUnsafe().setSelSize(1);
    const auto nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
    nodeIDVector->setState(chunkState);
    nodeIDVector->setValue<internalID_t>(0,
        internalID_t{deletionRecord.nodeOffset, deletionRecord.tableID});
    const auto deleteState =
        std::make_unique<NodeTableDeleteState>(*nodeIDVector, *deletionRecord.ownedPKVector);
    KU_ASSERT(clientContext.getTx() && clientContext.getTx()->isRecovery());
    table.delete_(clientContext.getTx(), *deleteState);
}

void WALReplayer::replayNodeUpdateRecord(const WALRecord& walRecord) const {
    const auto& updateRecord = walRecord.constCast<NodeUpdateRecord>();
    const auto tableID = updateRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<NodeTable>();
    const auto chunkState = std::make_shared<DataChunkState>();
    chunkState->getSelVectorUnsafe().setSelSize(1);
    const auto nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
    nodeIDVector->setState(chunkState);
    nodeIDVector->setValue<internalID_t>(0,
        internalID_t{updateRecord.nodeOffset, updateRecord.tableID});
    updateRecord.ownedPropertyVector->setState(chunkState);
    const auto updateState = std::make_unique<NodeTableUpdateState>(updateRecord.columnID,
        *nodeIDVector, *updateRecord.ownedPropertyVector);
    KU_ASSERT(clientContext.getTx() && clientContext.getTx()->isRecovery());
    table.update(clientContext.getTx(), *updateState);
}

void WALReplayer::replayRelDeletionRecord(const WALRecord& walRecord) const {
    const auto& deletionRecord = walRecord.constCast<RelDeletionRecord>();
    const auto tableID = deletionRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<RelTable>();
    const auto chunkState = std::make_shared<DataChunkState>();
    chunkState->getSelVectorUnsafe().setSelSize(1);
    deletionRecord.ownedSrcNodeIDVector->setState(chunkState);
    deletionRecord.ownedDstNodeIDVector->setState(chunkState);
    deletionRecord.ownedRelIDVector->setState(chunkState);
    const auto deleteState =
        std::make_unique<RelTableDeleteState>(*deletionRecord.ownedSrcNodeIDVector,
            *deletionRecord.ownedDstNodeIDVector, *deletionRecord.ownedRelIDVector);
    KU_ASSERT(clientContext.getTx() && clientContext.getTx()->isRecovery());
    table.delete_(clientContext.getTx(), *deleteState);
}

void WALReplayer::replayRelUpdateRecord(const WALRecord& walRecord) const {
    const auto& updateRecord = walRecord.constCast<RelUpdateRecord>();
    const auto tableID = updateRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<RelTable>();
    const auto chunkState = std::make_shared<DataChunkState>();
    chunkState->getSelVectorUnsafe().setSelSize(1);
    updateRecord.ownedSrcNodeIDVector->setState(chunkState);
    updateRecord.ownedDstNodeIDVector->setState(chunkState);
    updateRecord.ownedRelIDVector->setState(chunkState);
    updateRecord.ownedPropertyVector->setState(chunkState);
    const auto updateState = std::make_unique<RelTableUpdateState>(updateRecord.columnID,
        *updateRecord.ownedSrcNodeIDVector, *updateRecord.ownedDstNodeIDVector,
        *updateRecord.ownedRelIDVector, *updateRecord.ownedPropertyVector);
    KU_ASSERT(clientContext.getTx() && clientContext.getTx()->isRecovery());
    table.update(clientContext.getTx(), *updateState);
}

void WALReplayer::replayCopyTableRecord(const WALRecord&) const {
    // DO NOTHING.
    // TODO(Guodong): FIX-ME.
}

void WALReplayer::replayUpdateSequenceRecord(const WALRecord& walRecord) const {
    if (!(isCheckpoint && isRecovering)) {
        return;
    }
    auto& dropEntryRecord = walRecord.constCast<UpdateSequenceRecord>();
    const auto sequenceID = dropEntryRecord.sequenceID;
    const auto entry =
        clientContext.getCatalog()->getSequenceCatalogEntry(&DUMMY_TRANSACTION, sequenceID);
    entry->replayVal(dropEntryRecord.data.usageCount, dropEntryRecord.data.currVal,
        dropEntryRecord.data.nextVal);
}

} // namespace storage
} // namespace kuzu
