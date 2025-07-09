#include "storage/wal/wal_replayer.h"

#include "binder/binder.h"
#include "catalog/catalog_entry/scalar_macro_catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "catalog/catalog_entry/type_catalog_entry.h"
#include "common/file_system/file_info.h"
#include "common/file_system/file_system.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "extension/extension_manager.h"
#include "main/client_context.h"
#include "processor/expression_mapper.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"
#include "storage/wal/wal_record.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

WALReplayer::WALReplayer(main::ClientContext& clientContext) : clientContext{clientContext} {
    walPath = StorageUtils::getWALFilePath(clientContext.getDatabasePath());
    shadowFilePath = StorageUtils::getShadowFilePath(clientContext.getDatabasePath());
}

void WALReplayer::replay() const {
    auto vfs = clientContext.getVFSUnsafe();
    Checkpointer checkpointer(clientContext);
    // First, check if the WAL file exists. If it does not, we can safely remove the shadow file.
    if (!vfs->fileOrPathExists(walPath, &clientContext)) {
        removeFileIfExists(shadowFilePath);
        // Read the checkpointed data from the disk.
        checkpointer.readCheckpoint();
        return;
    }
    // If the WAL file exists, we need to replay it.
    auto fileInfo =
        clientContext.getVFSUnsafe()->openFile(walPath, FileOpenFlags(FileFlags::READ_ONLY));
    // Check if the wal file is empty. If so, we do not need to replay anything.
    if (fileInfo->getFileSize() == 0) {
        removeWALAndShadowFiles();
        // Read the checkpointed data from the disk.
        checkpointer.readCheckpoint();
        return;
    }
    // Start replaying the WAL records.
    try {
        // First, we dry run the replay to find out the offset of the last record that was
        // CHECKPOINT or COMMIT.
        auto [offsetDeserialized, isLastRecordCheckpoint] = dryReplay();
        if (isLastRecordCheckpoint) {
            // If the last record is a checkpoint, we resume by replaying the shadow file.
            auto shadowFileInfo =
                vfs->openFile(shadowFilePath, FileOpenFlags(FileFlags::READ_ONLY));
            ShadowFile::replayShadowPageRecords(clientContext, std::move(shadowFileInfo));
            removeWALAndShadowFiles();
            // Re-read checkpointed data from disk again as now the shadow file is applied.
            clientContext.getStorageManager()->initDataFileHandle(vfs, &clientContext);
            checkpointer.readCheckpoint();
        } else {
            // There is no checkpoint record, so we should remove the shadow file if it exists.
            removeFileIfExists(shadowFilePath);
            // Read the checkpointed data from the disk.
            checkpointer.readCheckpoint();
            // Resume by replaying the WAL file from the beginning until the last COMMIT record.
            Deserializer deserializer(std::make_unique<BufferedFileReader>(std::move(fileInfo)));
            while (deserializer.getReader()->cast<BufferedFileReader>()->getReadOffset() <
                   offsetDeserialized) {
                KU_ASSERT(!deserializer.finished());
                auto walRecord = WALRecord::deserialize(deserializer, clientContext);
                replayWALRecord(*walRecord);
            }
        }
    } catch (const std::exception&) {
        if (clientContext.getTransactionContext()->hasActiveTransaction()) {
            // Handle the case that some transaction went during replaying. We should roll back
            // under this case. Usually this shouldn't happen, but it is possible if we have a bug
            // with the replay logic. This is to handle cases like that so we don't corrupt
            // transactions that have been replayed.
            clientContext.getTransactionContext()->rollback();
        }
        throw;
    }
}

WALReplayer::WALReplayInfo WALReplayer::dryReplay() const {
    uint64_t offsetDeserialized = 0;
    bool isLastRecordCheckpoint = false;
    try {
        auto fileInfo =
            clientContext.getVFSUnsafe()->openFile(walPath, FileOpenFlags(FileFlags::READ_ONLY));
        Deserializer deserializer(std::make_unique<BufferedFileReader>(std::move(fileInfo)));
        bool finishedDeserializing = deserializer.finished();
        while (!finishedDeserializing) {
            auto walRecord = WALRecord::deserialize(deserializer, clientContext);
            finishedDeserializing = deserializer.finished();
            switch (walRecord->type) {
            case WALRecordType::CHECKPOINT_RECORD: {
                KU_ASSERT(finishedDeserializing);
                // If we reach a checkpoint record, we can stop replaying.
                isLastRecordCheckpoint = true;
                finishedDeserializing = true;
                offsetDeserialized =
                    deserializer.getReader()->cast<BufferedFileReader>()->getReadOffset();
            } break;
            case WALRecordType::COMMIT_RECORD: {
                // Update the offset to the end of the last commit record.
                offsetDeserialized =
                    deserializer.getReader()->cast<BufferedFileReader>()->getReadOffset();
            } break;
            default: {
                // DO NOTHING.
            }
            }
        }
    } catch (...) { // NOLINT
        // If we hit an exception while deserializing, we assume that the WAL file is (partially)
        // corrupted. This should only happen for records of the last transaction recorded.
    }
    return {offsetDeserialized, isLastRecordCheckpoint};
}

void WALReplayer::replayWALRecord(WALRecord& walRecord) const {
    switch (walRecord.type) {
    case WALRecordType::BEGIN_TRANSACTION_RECORD: {
        clientContext.getTransactionContext()->beginRecoveryTransaction();
    } break;
    case WALRecordType::COMMIT_RECORD: {
        clientContext.getTransactionContext()->commit();
    } break;
    case WALRecordType::CREATE_CATALOG_ENTRY_RECORD: {
        replayCreateCatalogEntryRecord(walRecord);
    } break;
    case WALRecordType::DROP_CATALOG_ENTRY_RECORD: {
        replayDropCatalogEntryRecord(walRecord);
    } break;
    case WALRecordType::ALTER_TABLE_ENTRY_RECORD: {
        replayAlterTableEntryRecord(walRecord);
    } break;
    case WALRecordType::TABLE_INSERTION_RECORD: {
        replayTableInsertionRecord(walRecord);
    } break;
    case WALRecordType::NODE_DELETION_RECORD: {
        replayNodeDeletionRecord(walRecord);
    } break;
    case WALRecordType::NODE_UPDATE_RECORD: {
        replayNodeUpdateRecord(walRecord);
    } break;
    case WALRecordType::REL_DELETION_RECORD: {
        replayRelDeletionRecord(walRecord);
    } break;
    case WALRecordType::REL_DETACH_DELETE_RECORD: {
        replayRelDetachDeletionRecord(walRecord);
    } break;
    case WALRecordType::REL_UPDATE_RECORD: {
        replayRelUpdateRecord(walRecord);
    } break;
    case WALRecordType::COPY_TABLE_RECORD: {
        replayCopyTableRecord(walRecord);
    } break;
    case WALRecordType::UPDATE_SEQUENCE_RECORD: {
        replayUpdateSequenceRecord(walRecord);
    } break;
    case WALRecordType::LOAD_EXTENSION_RECORD: {
        replayLoadExtensionRecord(walRecord);
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

void WALReplayer::replayCreateCatalogEntryRecord(WALRecord& walRecord) const {
    auto catalog = clientContext.getCatalog();
    auto transaction = clientContext.getTransaction();
    auto storageManager = clientContext.getStorageManager();
    auto& record = walRecord.cast<CreateCatalogEntryRecord>();
    switch (record.ownedCatalogEntry->getType()) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY: {
        auto& entry = record.ownedCatalogEntry->constCast<TableCatalogEntry>();
        auto newEntry = catalog->createTableEntry(transaction,
            entry.getBoundCreateTableInfo(transaction, record.isInternal));
        storageManager->createTable(newEntry->ptrCast<TableCatalogEntry>());
    } break;
    case CatalogEntryType::SCALAR_MACRO_ENTRY: {
        auto& macroEntry = record.ownedCatalogEntry->constCast<ScalarMacroCatalogEntry>();
        catalog->addScalarMacroFunction(transaction, macroEntry.getName(),
            macroEntry.getMacroFunction()->copy());
    } break;
    case CatalogEntryType::SEQUENCE_ENTRY: {
        auto& sequenceEntry = record.ownedCatalogEntry->constCast<SequenceCatalogEntry>();
        catalog->createSequence(transaction,
            sequenceEntry.getBoundCreateSequenceInfo(record.isInternal));
    } break;
    case CatalogEntryType::TYPE_ENTRY: {
        auto& typeEntry = record.ownedCatalogEntry->constCast<TypeCatalogEntry>();
        catalog->createType(transaction, typeEntry.getName(), typeEntry.getLogicalType().copy());
    } break;
    case CatalogEntryType::INDEX_ENTRY: {
        catalog->createIndex(transaction, std::move(record.ownedCatalogEntry));
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void WALReplayer::replayDropCatalogEntryRecord(const WALRecord& walRecord) const {
    auto& dropEntryRecord = walRecord.constCast<DropCatalogEntryRecord>();
    auto catalog = clientContext.getCatalog();
    auto transaction = clientContext.getTransaction();
    const auto entryID = dropEntryRecord.entryID;
    switch (dropEntryRecord.entryType) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY: {
        KU_ASSERT(clientContext.getCatalog());
        catalog->dropTableEntry(transaction, entryID);
    } break;
    case CatalogEntryType::SEQUENCE_ENTRY: {
        catalog->dropSequence(transaction, entryID);
    } break;
    case CatalogEntryType::INDEX_ENTRY: {
        catalog->dropIndex(transaction, entryID);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void WALReplayer::replayAlterTableEntryRecord(const WALRecord& walRecord) const {
    auto binder = Binder(&clientContext);
    auto& alterEntryRecord = walRecord.constCast<AlterTableEntryRecord>();
    auto catalog = clientContext.getCatalog();
    auto transaction = clientContext.getTransaction();
    auto storageManager = clientContext.getStorageManager();
    auto ownedAlterInfo = alterEntryRecord.ownedAlterInfo.get();
    catalog->alterTableEntry(transaction, *ownedAlterInfo);
    auto& pageAllocator = *clientContext.getStorageManager()->getDataFH()->getPageManager();
    switch (ownedAlterInfo->alterType) {
    case AlterType::ADD_PROPERTY: {
        const auto exprBinder = binder.getExpressionBinder();
        const auto addInfo = ownedAlterInfo->extraInfo->constPtrCast<BoundExtraAddPropertyInfo>();
        // We don't implicit cast here since it must already be done the first time
        const auto boundDefault =
            exprBinder->bindExpression(*addInfo->propertyDefinition.defaultExpr);
        auto exprMapper = ExpressionMapper();
        const auto defaultValueEvaluator = exprMapper.getEvaluator(boundDefault);
        defaultValueEvaluator->init(ResultSet(0) /* dummy ResultSet */, &clientContext);
        const auto entry = catalog->getTableCatalogEntry(transaction, ownedAlterInfo->tableName);
        const auto& addedProp = entry->getProperty(addInfo->propertyDefinition.getName());
        TableAddColumnState state{addedProp, *defaultValueEvaluator};
        KU_ASSERT(clientContext.getStorageManager());
        switch (entry->getTableType()) {
        case TableType::REL: {
            for (auto& relEntryInfo : entry->cast<RelGroupCatalogEntry>().getRelEntryInfos()) {
                storageManager->getTable(relEntryInfo.oid)
                    ->addColumn(transaction, state, pageAllocator);
            }
        } break;
        case TableType::NODE: {
            storageManager->getTable(entry->getTableID())
                ->addColumn(transaction, state, pageAllocator);
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
    } break;
    case AlterType::ADD_FROM_TO_CONNECTION: {
        auto extraInfo = ownedAlterInfo->extraInfo->constPtrCast<BoundExtraAlterFromToConnection>();
        auto relGroupEntry = catalog->getTableCatalogEntry(transaction, ownedAlterInfo->tableName)
                                 ->ptrCast<RelGroupCatalogEntry>();
        auto relEntryInfo =
            relGroupEntry->getRelEntryInfo(extraInfo->fromTableID, extraInfo->toTableID);
        storageManager->addRelTable(relGroupEntry, *relEntryInfo);
    } break;
    default:
        break;
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
    KU_ASSERT(!insertionRecord.ownedVectors.empty());
    const auto anchorState = insertionRecord.ownedVectors[0]->state;
    const auto numNodes = anchorState->getSelVector().getSelSize();
    for (auto i = 0u; i < insertionRecord.ownedVectors.size(); i++) {
        insertionRecord.ownedVectors[i]->setState(anchorState);
    }
    std::vector<ValueVector*> propertyVectors(insertionRecord.ownedVectors.size());
    for (auto i = 0u; i < insertionRecord.ownedVectors.size(); i++) {
        propertyVectors[i] = insertionRecord.ownedVectors[i].get();
    }
    KU_ASSERT(table.getPKColumnID() < insertionRecord.ownedVectors.size());
    auto& pkVector = *insertionRecord.ownedVectors[table.getPKColumnID()];
    const auto nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
    nodeIDVector->setState(anchorState);
    const auto insertState =
        std::make_unique<NodeTableInsertState>(*nodeIDVector, pkVector, propertyVectors);
    KU_ASSERT(clientContext.getTransaction() && clientContext.getTransaction()->isRecovery());
    table.initInsertState(&clientContext, *insertState);
    anchorState->getSelVectorUnsafe().setToFiltered(1);
    for (auto i = 0u; i < numNodes; i++) {
        anchorState->getSelVectorUnsafe()[0] = i;
        table.insert(clientContext.getTransaction(), *insertState);
    }
}

void WALReplayer::replayRelTableInsertRecord(const WALRecord& walRecord) const {
    const auto& insertionRecord = walRecord.constCast<TableInsertionRecord>();
    const auto tableID = insertionRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<RelTable>();
    KU_ASSERT(!insertionRecord.ownedVectors.empty());
    const auto anchorState = insertionRecord.ownedVectors[0]->state;
    const auto numRels = anchorState->getSelVector().getSelSize();
    anchorState->getSelVectorUnsafe().setToFiltered(1);
    for (auto i = 0u; i < insertionRecord.ownedVectors.size(); i++) {
        insertionRecord.ownedVectors[i]->setState(anchorState);
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
    KU_ASSERT(clientContext.getTransaction() && clientContext.getTransaction()->isRecovery());
    for (auto i = 0u; i < numRels; i++) {
        anchorState->getSelVectorUnsafe()[0] = i;
        table.initInsertState(&clientContext, *insertState);
        table.insert(clientContext.getTransaction(), *insertState);
    }
}

void WALReplayer::replayNodeDeletionRecord(const WALRecord& walRecord) const {
    const auto& deletionRecord = walRecord.constCast<NodeDeletionRecord>();
    const auto tableID = deletionRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<NodeTable>();
    const auto anchorState = deletionRecord.ownedPKVector->state;
    KU_ASSERT(anchorState->getSelVector().getSelSize() == 1);
    const auto nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
    nodeIDVector->setState(anchorState);
    nodeIDVector->setValue<internalID_t>(0,
        internalID_t{deletionRecord.nodeOffset, deletionRecord.tableID});
    const auto deleteState =
        std::make_unique<NodeTableDeleteState>(*nodeIDVector, *deletionRecord.ownedPKVector);
    KU_ASSERT(clientContext.getTransaction() && clientContext.getTransaction()->isRecovery());
    table.delete_(clientContext.getTransaction(), *deleteState);
}

void WALReplayer::replayNodeUpdateRecord(const WALRecord& walRecord) const {
    const auto& updateRecord = walRecord.constCast<NodeUpdateRecord>();
    const auto tableID = updateRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<NodeTable>();
    const auto anchorState = updateRecord.ownedPropertyVector->state;
    KU_ASSERT(anchorState->getSelVector().getSelSize() == 1);
    const auto nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
    nodeIDVector->setState(anchorState);
    nodeIDVector->setValue<internalID_t>(0,
        internalID_t{updateRecord.nodeOffset, updateRecord.tableID});
    const auto updateState = std::make_unique<NodeTableUpdateState>(updateRecord.columnID,
        *nodeIDVector, *updateRecord.ownedPropertyVector);
    KU_ASSERT(clientContext.getTransaction() && clientContext.getTransaction()->isRecovery());
    table.update(clientContext.getTransaction(), *updateState);
}

void WALReplayer::replayRelDeletionRecord(const WALRecord& walRecord) const {
    const auto& deletionRecord = walRecord.constCast<RelDeletionRecord>();
    const auto tableID = deletionRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<RelTable>();
    const auto anchorState = deletionRecord.ownedRelIDVector->state;
    KU_ASSERT(anchorState->getSelVector().getSelSize() == 1);
    const auto deleteState =
        std::make_unique<RelTableDeleteState>(*deletionRecord.ownedSrcNodeIDVector,
            *deletionRecord.ownedDstNodeIDVector, *deletionRecord.ownedRelIDVector);
    KU_ASSERT(clientContext.getTransaction() && clientContext.getTransaction()->isRecovery());
    table.delete_(clientContext.getTransaction(), *deleteState);
}

void WALReplayer::replayRelDetachDeletionRecord(const WALRecord& walRecord) const {
    const auto& deletionRecord = walRecord.constCast<RelDetachDeleteRecord>();
    const auto tableID = deletionRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<RelTable>();
    KU_ASSERT(clientContext.getTransaction() && clientContext.getTransaction()->isRecovery());
    const auto anchorState = deletionRecord.ownedSrcNodeIDVector->state;
    KU_ASSERT(anchorState->getSelVector().getSelSize() == 1);
    const auto dstNodeIDVector =
        std::make_unique<ValueVector>(LogicalType{LogicalTypeID::INTERNAL_ID});
    const auto relIDVector = std::make_unique<ValueVector>(LogicalType{LogicalTypeID::INTERNAL_ID});
    dstNodeIDVector->setState(anchorState);
    relIDVector->setState(anchorState);
    const auto deleteState = std::make_unique<RelTableDeleteState>(
        *deletionRecord.ownedSrcNodeIDVector, *dstNodeIDVector, *relIDVector);
    table.detachDelete(clientContext.getTransaction(), deletionRecord.direction, deleteState.get());
}

void WALReplayer::replayRelUpdateRecord(const WALRecord& walRecord) const {
    const auto& updateRecord = walRecord.constCast<RelUpdateRecord>();
    const auto tableID = updateRecord.tableID;
    auto& table = clientContext.getStorageManager()->getTable(tableID)->cast<RelTable>();
    const auto anchorState = updateRecord.ownedRelIDVector->state;
    KU_ASSERT(anchorState == updateRecord.ownedSrcNodeIDVector->state &&
              anchorState == updateRecord.ownedSrcNodeIDVector->state &&
              anchorState == updateRecord.ownedPropertyVector->state);
    KU_ASSERT(anchorState->getSelVector().getSelSize() == 1);
    const auto updateState = std::make_unique<RelTableUpdateState>(updateRecord.columnID,
        *updateRecord.ownedSrcNodeIDVector, *updateRecord.ownedDstNodeIDVector,
        *updateRecord.ownedRelIDVector, *updateRecord.ownedPropertyVector);
    KU_ASSERT(clientContext.getTransaction() && clientContext.getTransaction()->isRecovery());
    table.update(clientContext.getTransaction(), *updateState);
}

void WALReplayer::replayCopyTableRecord(const WALRecord&) const {
    // DO NOTHING.
}

void WALReplayer::replayUpdateSequenceRecord(const WALRecord& walRecord) const {
    auto& sequenceEntryRecord = walRecord.constCast<UpdateSequenceRecord>();
    const auto sequenceID = sequenceEntryRecord.sequenceID;
    const auto entry =
        clientContext.getCatalog()->getSequenceEntry(clientContext.getTransaction(), sequenceID);
    entry->nextKVal(clientContext.getTransaction(), sequenceEntryRecord.kCount);
}

void WALReplayer::replayLoadExtensionRecord(const WALRecord& walRecord) const {
    const auto& loadExtensionRecord = walRecord.constCast<LoadExtensionRecord>();
    clientContext.getExtensionManager()->loadExtension(loadExtensionRecord.path, &clientContext);
}

void WALReplayer::removeWALAndShadowFiles() const {
    removeFileIfExists(shadowFilePath);
    removeFileIfExists(walPath);
}

void WALReplayer::removeFileIfExists(const std::string& path) const {
    if (clientContext.getStorageManager()->isReadOnly()) {
        return;
    }
    auto vfs = clientContext.getVFSUnsafe();
    if (vfs->fileOrPathExists(path, &clientContext)) {
        vfs->removeFileIfExists(path);
    }
}

} // namespace storage
} // namespace kuzu
