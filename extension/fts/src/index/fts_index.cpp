#include "index/fts_index.h"

#include "catalog/catalog.h"
#include "catalog/fts_index_catalog_entry.h"
#include "index/fts_update_state.h"
#include "re2.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::storage;
using namespace kuzu::common;
using namespace kuzu::transaction;

FTSIndex::FTSIndex(IndexInfo indexInfo, std::unique_ptr<IndexStorageInfo> storageInfo,
    FTSConfig config, main::ClientContext* context)
    : Index{indexInfo, std::move(storageInfo)},
      internalTableInfo{context, indexInfo.tableID, indexInfo.name, config.stopWordsTableName},
      config{std::move(config)} {}

std::unique_ptr<Index> FTSIndex::load(main::ClientContext* context, StorageManager*,
    IndexInfo indexInfo, std::span<uint8_t> storageInfoBuffer) {
    auto catalog = context->getCatalog();
    auto reader =
        std::make_unique<BufferReader>(storageInfoBuffer.data(), storageInfoBuffer.size());
    auto storageInfo = FTSStorageInfo::deserialize(std::move(reader));
    auto indexEntry =
        catalog->getIndex(context->getTransaction(), indexInfo.tableID, indexInfo.name);
    auto ftsConfig = indexEntry->getAuxInfo().cast<FTSIndexAuxInfo>().config;
    return std::make_unique<FTSIndex>(std::move(indexInfo), std::move(storageInfo),
        std::move(ftsConfig), context);
}

struct TermInfo {
    offset_t offset;
    uint64_t tf;
};

std::shared_ptr<BufferWriter> FTSStorageInfo::serialize() const {
    auto bufferWriter = std::make_shared<BufferWriter>();
    auto serializer = Serializer(bufferWriter);
    serializer.write<idx_t>(numDocs);
    serializer.write<double>(avgDocLen);
    serializer.write<offset_t>(numCheckpointedNodes);
    return bufferWriter;
}

std::unique_ptr<IndexStorageInfo> FTSStorageInfo::deserialize(
    std::unique_ptr<BufferReader> reader) {
    idx_t numDocs = 0;
    double avgDocLen = 0.0;
    offset_t numCheckpointedNodes = 0;
    Deserializer deSer{std::move(reader)};
    deSer.deserializeValue<idx_t>(numDocs);
    deSer.deserializeValue<double>(avgDocLen);
    deSer.deserializeValue<offset_t>(numCheckpointedNodes);
    return std::make_unique<FTSStorageInfo>(numDocs, avgDocLen, numCheckpointedNodes);
}

std::unique_ptr<Index::InsertState> FTSIndex::initInsertState(main::ClientContext* context,
    visible_func /*isVisible*/) {
    return std::make_unique<FTSInsertState>(context, internalTableInfo);
}

static std::vector<std::string> getTerms(Transaction* transaction, FTSConfig& config,
    NodeTable* stopWordsTable, const std::vector<ValueVector*>& indexVectors, sel_t pos,
    MemoryManager* mm) {
    std::string content;
    std::vector<std::string> terms;
    RE2 regexPattern{config.ignorePattern};
    for (auto indexVector : indexVectors) {
        if (indexVector->isNull(pos)) {
            continue;
        }
        content = indexVector->getValue<ku_string_t>(pos).getAsString();
        FTSUtils::normalizeQuery(content, regexPattern);
        auto termsInContent =
            StringUtils::split(content, " " /* delimiter */, true /* ignoreEmptyStringParts */);
        termsInContent = FTSUtils::stemTerms(termsInContent, config, mm, stopWordsTable,
            transaction, true /* isConjunctive */, false /* isQuery */);
        // TODO(Ziyi): StringUtils::split() has a bug which doesn't ignore empty parts even
        // ignoreEmptyStringParts is set to true.
        for (auto& term : termsInContent) {
            if (term.empty()) {
                continue;
            }
            terms.push_back(term);
        }
    }
    return terms;
}

struct DocInfo {
    std::unordered_map<std::string, TermInfo> termInfos;
    uint64_t docLen;

    DocInfo(Transaction* transaction, FTSConfig& config, NodeTable* stopWordsTable,
        const std::vector<ValueVector*>& indexVectors, sel_t pos, MemoryManager* mm);
};

DocInfo::DocInfo(Transaction* transaction, FTSConfig& config, NodeTable* stopWordsTable,
    const std::vector<ValueVector*>& indexVectors, sel_t pos, MemoryManager* mm) {
    auto terms = getTerms(transaction, config, stopWordsTable, indexVectors, pos, mm);
    for (auto& term : terms) {
        termInfos[term].tf++;
    }
    docLen = terms.size();
}

void FTSIndex::insert(Transaction* transaction, const ValueVector& nodeIDVector,
    const std::vector<ValueVector*>& indexVectors, InsertState& insertState) {
    auto totalInsertedDocLen = 0u;
    auto& ftsInsertState = insertState.cast<FTSInsertState>();
    for (auto i = 0u; i < nodeIDVector.state->getSelSize(); i++) {
        auto pos = nodeIDVector.state->getSelVector()[i];
        DocInfo docInfo{transaction, config, internalTableInfo.stopWordsTable, indexVectors, pos,
            ftsInsertState.updateVectors.mm};
        if (docInfo.termInfos.size() == 0) {
            break;
        }
        auto insertedDocID = insertToDocTable(transaction, ftsInsertState,
            nodeIDVector.getValue<nodeID_t>(pos), docInfo.docLen);
        totalInsertedDocLen += docInfo.docLen;
        insertToTermsTable(transaction, docInfo.termInfos, ftsInsertState);
        insertToAppearsInTable(transaction, docInfo.termInfos, ftsInsertState, insertedDocID,
            internalTableInfo.termsTable->getTableID());
    }
    auto& ftsStorageInfo = storageInfo->cast<FTSStorageInfo>();
    auto numInsertedDocs = nodeIDVector.state->getSelSize();
    ftsStorageInfo.avgDocLen =
        (ftsStorageInfo.avgDocLen * ftsStorageInfo.numDocs + totalInsertedDocLen) /
        (ftsStorageInfo.numDocs + numInsertedDocs);
    ftsStorageInfo.numDocs += numInsertedDocs;
    ftsStorageInfo.numCheckpointedNodes =
        nodeIDVector.getValue<nodeID_t>(nodeIDVector.state->getSelVector()[0]).offset + 1;
}

std::unique_ptr<Index::UpdateState> FTSIndex::initUpdateState(main::ClientContext* context,
    column_id_t columnID, storage::visible_func isVisible) {
    auto ftsUpdateState =
        std::make_unique<FTSUpdateState>(context, internalTableInfo, indexInfo.columnIDs, columnID);
    ftsUpdateState->ftsInsertState = initInsertState(context, isVisible);
    ftsUpdateState->ftsDeleteState =
        initDeleteState(context->getTransaction(), context->getMemoryManager(), isVisible);
    return ftsUpdateState;
}

void FTSIndex::update(Transaction* transaction, const common::ValueVector& nodeIDVector,
    ValueVector& propertyVector, UpdateState& updateState) {
    auto& ftsUpdateState = updateState.cast<FTSUpdateState>();
    auto nodeToUpdate = nodeIDVector.getValue<nodeID_t>(nodeIDVector.state->getSelVector()[0]);
    delete_(transaction, nodeIDVector, *ftsUpdateState.ftsDeleteState);

    auto& indexTableScanState = ftsUpdateState.indexTableState.scanState;
    ftsUpdateState.nodeIDVector.setValue(0, nodeToUpdate);
    internalTableInfo.table->initScanState(transaction, *indexTableScanState, nodeToUpdate.tableID,
        nodeToUpdate.offset);
    internalTableInfo.table->lookup(transaction, *indexTableScanState);
    ftsUpdateState.indexTableState.indexVectors[ftsUpdateState.columnIdxWithUpdate] =
        &propertyVector;
    insert(transaction, nodeIDVector, ftsUpdateState.indexTableState.indexVectors,
        *ftsUpdateState.ftsInsertState);
}

std::unique_ptr<Index::DeleteState> FTSIndex::initDeleteState(const Transaction* transaction,
    MemoryManager* mm, visible_func /*isVisible*/) {
    return std::make_unique<FTSDeleteState>(mm, transaction, internalTableInfo,
        indexInfo.columnIDs);
}

void FTSIndex::delete_(Transaction* transaction, const ValueVector& nodeIDVector,
    DeleteState& deleteState) {
    auto& ftsDeleteState = deleteState.cast<FTSDeleteState>();
    auto& ftsStorageInfo = storageInfo->cast<FTSStorageInfo>();
    double totalDocLen = ftsStorageInfo.avgDocLen * ftsStorageInfo.numDocs;
    for (auto i = 0u; i < nodeIDVector.state->getSelSize(); i++) {
        auto pos = nodeIDVector.state->getSelVector()[i];
        auto deletedNodeID = nodeIDVector.getValue<nodeID_t>(pos);
        ftsDeleteState.updateVectors.idVector.setValue(0, deletedNodeID);
        internalTableInfo.table->initScanState(transaction,
            *ftsDeleteState.indexTableState.scanState, deletedNodeID.tableID, deletedNodeID.offset);
        internalTableInfo.table->lookup(transaction, *ftsDeleteState.indexTableState.scanState);
        DocInfo docInfo{transaction, config, internalTableInfo.stopWordsTable,
            ftsDeleteState.indexTableState.indexVectors, 0, ftsDeleteState.updateVectors.mm};
        if (docInfo.termInfos.size() == 0) {
            continue;
        }
        auto deletedDocID =
            deleteFromDocTable(transaction, ftsDeleteState, deletedNodeID, totalDocLen);
        deleteFromTermsTable(transaction, docInfo.termInfos, ftsDeleteState);
        deleteFromAppearsInTable(transaction, ftsDeleteState, deletedDocID);
    }
    auto numDeletedDocs = nodeIDVector.state->getSelSize();
    if (ftsStorageInfo.numDocs == numDeletedDocs) {
        ftsStorageInfo.avgDocLen = 0;
    } else {
        ftsStorageInfo.avgDocLen = totalDocLen / (ftsStorageInfo.numDocs - numDeletedDocs);
    }
    ftsStorageInfo.numDocs -= numDeletedDocs;
}

void FTSIndex::finalize(main::ClientContext* context) {
    auto& ftsStorageInfo = storageInfo->cast<FTSStorageInfo>();
    const auto numTotalRows =
        internalTableInfo.table->getNumTotalRows(&DUMMY_CHECKPOINT_TRANSACTION);
    if (numTotalRows == ftsStorageInfo.numCheckpointedNodes) {
        return;
    }
    auto transaction = context->getTransaction();
    auto dataChunk = DataChunkState::getSingleValueDataChunkState();
    ValueVector idVector{LogicalType::INTERNAL_ID(), context->getMemoryManager(), dataChunk};
    IndexTableState indexTableState{context->getMemoryManager(), transaction, internalTableInfo,
        indexInfo.columnIDs, idVector, dataChunk};
    internalID_t insertedNodeID = {INVALID_OFFSET, internalTableInfo.table->getTableID()};
    for (auto offset = ftsStorageInfo.numCheckpointedNodes; offset < numTotalRows; offset++) {
        insertedNodeID.offset = offset;
        idVector.setValue(0, insertedNodeID);
        internalTableInfo.table->initScanState(transaction, *indexTableState.scanState,
            insertedNodeID.tableID, insertedNodeID.offset);
        internalTableInfo.table->lookup(transaction, *indexTableState.scanState);
        auto insertState = initInsertState(context, [](offset_t) { return true; });
        insert(transaction, idVector, indexTableState.indexVectors, *insertState);
    }
    ftsStorageInfo.numCheckpointedNodes = numTotalRows;
}

void FTSIndex::checkpoint(main::ClientContext* context, storage::PageAllocator& pageAllocator) {
    KU_ASSERT(!context->isInMemory());
    auto catalog = context->getCatalog();
    internalTableInfo.docTable->checkpoint(context,
        catalog->getTableCatalogEntry(&DUMMY_CHECKPOINT_TRANSACTION,
            internalTableInfo.docTable->getTableID()),
        pageAllocator);
    internalTableInfo.termsTable->checkpoint(context,
        catalog->getTableCatalogEntry(&DUMMY_CHECKPOINT_TRANSACTION,
            internalTableInfo.termsTable->getTableID()),
        pageAllocator);
    auto appearsInTableName =
        FTSUtils::getAppearsInTableName(internalTableInfo.table->getTableID(), indexInfo.name);
    auto appearsInTableEntry =
        catalog->getTableCatalogEntry(&DUMMY_CHECKPOINT_TRANSACTION, appearsInTableName);
    internalTableInfo.appearsInfoTable->checkpoint(context, appearsInTableEntry, pageAllocator);
}

nodeID_t FTSIndex::insertToDocTable(Transaction* transaction, FTSInsertState& insertState,
    nodeID_t insertedNodeID, uint64_t docLen) const {
    auto& ftsInsertState = insertState.cast<FTSInsertState>();
    ftsInsertState.updateVectors.int64PKVector.setValue(0, insertedNodeID.offset);
    ftsInsertState.updateVectors.uint64PropVector.setValue(0, static_cast<uint64_t>(docLen));
    auto docTable = internalTableInfo.docTable;
    docTable->insert(transaction, ftsInsertState.docTableInsertState);
    auto& docIDVector = ftsInsertState.updateVectors.idVector;
    return docIDVector.getValue<nodeID_t>(docIDVector.state->getSelVector()[0]);
}

void FTSIndex::insertToTermsTable(Transaction* transaction,
    std::unordered_map<std::string, TermInfo>& tfCollection, FTSInsertState& ftsInsertState) const {
    auto termsTable = internalTableInfo.termsTable;
    internalID_t termNodeID{INVALID_OFFSET, termsTable->getTableID()};
    for (auto& [term, termInfo] : tfCollection) {
        auto& termPKVector = ftsInsertState.updateVectors.stringPKVector;
        termPKVector.setValue(0, term);
        // If the term already exists in the terms table, we update the df. Otherwise, we
        // insert a new term entry to the terms table.
        auto& termIDVector = ftsInsertState.updateVectors.idVector;
        auto& dfVector = ftsInsertState.updateVectors.uint64PropVector;
        if (termsTable->lookupPK(transaction, &termPKVector, 0 /* vectorPos */,
                termNodeID.offset)) {
            termIDVector.setValue(0, termNodeID);
            termsTable->initScanState(transaction,
                ftsInsertState.termsTableState.termsTableScanState, termNodeID.tableID,
                termNodeID.offset);
            termsTable->lookup(transaction, ftsInsertState.termsTableState.termsTableScanState);
            dfVector.setValue(0, dfVector.getValue<uint64_t>(0) + 1);
            termsTable->update(transaction, ftsInsertState.termsTableState.termsTableUpdateState);
            termInfo.offset = termNodeID.offset;
        } else {
            dfVector.setValue(0, 1);
            termsTable->insert(transaction, ftsInsertState.termsTableInsertState);
            termInfo.offset = termIDVector.getValue<nodeID_t>(0).offset;
        }
    }
}

void FTSIndex::insertToAppearsInTable(Transaction* transaction,
    const std::unordered_map<std::string, TermInfo>& tfCollection, FTSInsertState& ftsInsertState,
    nodeID_t docID, table_id_t termsTableID) const {
    ftsInsertState.updateVectors.dstIDVector.setValue(0, docID);
    for (auto& [term, termInfo] : tfCollection) {
        ftsInsertState.updateVectors.srcIDVector.setValue(0,
            nodeID_t{termInfo.offset, termsTableID});
        ftsInsertState.updateVectors.uint64PropVector.setValue(0, termInfo.tf);
        internalTableInfo.appearsInfoTable->insert(transaction,
            ftsInsertState.appearsInTableInsertState);
    }
}

nodeID_t FTSIndex::deleteFromDocTable(Transaction* transaction, FTSDeleteState& deleteState,
    nodeID_t deletedNodeID, double& totalDocLen) const {
    auto& pkVector = deleteState.updateVectors.int64PKVector;
    pkVector.setValue(0, deletedNodeID.offset);
    nodeID_t docNodeID{INVALID_OFFSET, internalTableInfo.docTable->getTableID()};
    internalTableInfo.docTable->lookupPK(transaction, &pkVector, 0 /* vectorPos */,
        docNodeID.offset);
    deleteState.updateVectors.idVector.setValue(0, docNodeID);
    internalTableInfo.docTable->initScanState(transaction, deleteState.docTableScanState,
        docNodeID.tableID, docNodeID.offset);
    internalTableInfo.docTable->lookup(transaction, deleteState.docTableScanState);
    totalDocLen -= deleteState.updateVectors.uint64PropVector.getValue<uint64_t>(0);
    internalTableInfo.docTable->delete_(transaction, deleteState.docTableDeleteState);
    return docNodeID;
}

void FTSIndex::deleteFromTermsTable(Transaction* transaction,
    std::unordered_map<std::string, TermInfo>& tfCollection, FTSDeleteState& ftsInsertState) const {
    auto termsTable = internalTableInfo.termsTable;
    internalID_t termNodeID{INVALID_OFFSET, termsTable->getTableID()};
    for (auto& [term, termInfo] : tfCollection) {
        auto& termPKVector = ftsInsertState.updateVectors.stringPKVector;
        termPKVector.setValue(0, term);
        // If the df of the term is > 1, we decrement the df by 1. Otherwise, we delete the entry.
        auto& termIDVector = ftsInsertState.updateVectors.idVector;
        auto& dfVector = ftsInsertState.updateVectors.uint64PropVector;
        termsTable->lookupPK(transaction, &termPKVector, 0 /* vectorPos */, termNodeID.offset);
        termIDVector.setValue(0, termNodeID);
        termsTable->initScanState(transaction, ftsInsertState.termsTableState.termsTableScanState,
            termNodeID.tableID, termNodeID.offset);
        termsTable->lookup(transaction, ftsInsertState.termsTableState.termsTableScanState);
        termInfo.offset = termNodeID.offset;
        auto df = dfVector.getValue<uint64_t>(0);
        if (df == 1) {
            termsTable->delete_(transaction, ftsInsertState.termsTableDeleteState);
        } else {
            dfVector.setValue(0, df - 1);
            termsTable->update(transaction, ftsInsertState.termsTableState.termsTableUpdateState);
        }
    }
}

void FTSIndex::deleteFromAppearsInTable(Transaction* transaction, FTSDeleteState& ftsDeleteState,
    nodeID_t docID) const {
    auto dataChunk = std::make_shared<DataChunkState>(DEFAULT_VECTOR_CAPACITY);
    ftsDeleteState.updateVectors.dstIDVector.state = dataChunk;
    ftsDeleteState.updateVectors.idVector.state = dataChunk;
    ftsDeleteState.updateVectors.srcIDVector.setValue(0, docID);
    ftsDeleteState.appearsInTableDeleteState.detachDeleteDirection = RelDataDirection::BWD;
    internalTableInfo.appearsInfoTable->detachDelete(transaction,
        &ftsDeleteState.appearsInTableDeleteState);
}

} // namespace fts_extension
} // namespace kuzu
