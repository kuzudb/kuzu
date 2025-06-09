#include "index/fts_index.h"

#include "catalog/fts_index_catalog_entry.h"
#include "index/fts_update_state.h"
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

std::unique_ptr<Index> FTSIndex::load(main::ClientContext* context,
    StorageManager* /*storageManager*/, IndexInfo indexInfo, std::span<uint8_t> storageInfoBuffer) {
    auto catalog = context->getCatalog();
    auto reader =
        std::make_unique<BufferReader>(storageInfoBuffer.data(), storageInfoBuffer.size());
    auto storageInfo = FTSStorageInfo::deserialize(std::move(reader));
    auto indexEntry = catalog->getIndex(&DUMMY_TRANSACTION, indexInfo.tableID, indexInfo.name);
    auto ftsConfig = indexEntry->getAuxInfo().cast<FTSIndexAuxInfo>().config;
    return std::make_unique<FTSIndex>(std::move(indexInfo), std::move(storageInfo),
        std::move(ftsConfig), context);
}

struct TermInfo {
    offset_t offset;
    uint64_t tf;
};

std::shared_ptr<BufferedSerializer> FTSStorageInfo::serialize() const {
    auto bufferWriter = std::make_shared<BufferedSerializer>();
    auto serializer = Serializer(bufferWriter);
    serializer.write<idx_t>(numDocs);
    serializer.write<double>(avgDocLen);
    return bufferWriter;
}

std::unique_ptr<IndexStorageInfo> FTSStorageInfo::deserialize(
    std::unique_ptr<BufferReader> reader) {
    idx_t numDocs = 0;
    double avgDocLen = 0.0;
    Deserializer deSer{std::move(reader)};
    deSer.deserializeValue<idx_t>(numDocs);
    deSer.deserializeValue<double>(avgDocLen);
    return std::make_unique<FTSStorageInfo>(numDocs, avgDocLen);
}

std::unique_ptr<Index::InsertState> FTSIndex::initInsertState(Transaction* transaction,
    MemoryManager* mm, visible_func /*isVisible*/) {
    return std::make_unique<FTSInsertState>(mm, transaction, internalTableInfo);
}

static std::vector<std::string> getTerms(Transaction* transaction, FTSConfig& config,
    NodeTable* stopWordsTable, const std::vector<ValueVector*>& indexVectors, sel_t pos,
    MemoryManager* mm) {
    std::string content;
    std::vector<std::string> terms;
    for (auto indexVector : indexVectors) {
        if (indexVector->isNull(pos)) {
            continue;
        }
        content = indexVector->getValue<ku_string_t>(pos).getAsString();
        FTSUtils::normalizeQuery(content);
        auto termsInContent = StringUtils::split(content, " ");
        termsInContent = FTSUtils::stemTerms(termsInContent, config, mm, stopWordsTable,
            transaction, true /* isConjunctive */);
        terms.insert(terms.end(), termsInContent.begin(), termsInContent.end());
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
    const std::vector<ValueVector*>& indexVectors, Index::InsertState& insertState) {
    auto totalInsertedDocLen = 0u;
    auto& ftsInsertState = insertState.cast<FTSInsertState>();
    for (auto i = 0u; i < nodeIDVector.state->getSelSize(); i++) {
        auto pos = nodeIDVector.state->getSelVector()[i];
        DocInfo docInfo{transaction, config, internalTableInfo.stopWordsTable, indexVectors, pos,
            ftsInsertState.updateVectors.mm};
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
}

std::unique_ptr<Index::DeleteState> FTSIndex::initDeleteState(const Transaction* transaction,
    MemoryManager* mm, visible_func /*isVisible*/) {
    return std::make_unique<FTSDeleteState>(mm, transaction, internalTableInfo,
        indexInfo.columnIDs);
}

void FTSIndex::delete_(Transaction* transaction, const ValueVector& nodeIDVector,
    Index::DeleteState& deleteState) {
    auto& ftsDeleteState = deleteState.cast<FTSDeleteState>();
    for (auto i = 0u; i < nodeIDVector.state->getSelSize(); i++) {
        auto pos = nodeIDVector.state->getSelVector()[i];
        auto deletedNodeID = nodeIDVector.getValue<nodeID_t>(pos);
        deleteFromDocTable(transaction, ftsDeleteState, deletedNodeID);
        ftsDeleteState.updateVectors.idVector.setValue(0, deletedNodeID);
        internalTableInfo.table->initScanState(transaction,
            *ftsDeleteState.indexTableState.scanState, deletedNodeID.tableID, deletedNodeID.offset);
        internalTableInfo.table->lookup(transaction, *ftsDeleteState.indexTableState.scanState);
        DocInfo docInfo{transaction, config, internalTableInfo.stopWordsTable,
            ftsDeleteState.indexTableState.indexVectors, pos, ftsDeleteState.updateVectors.mm};
        deleteFromTermsTable(transaction, docInfo.termInfos, ftsDeleteState);
        deleteFromAppearsInTable(transaction, ftsDeleteState, deletedNodeID);
    }
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
    nodeID_t deletedNodeID) const {
    auto& pkVector = deleteState.updateVectors.int64PKVector;
    pkVector.setValue(0, deletedNodeID.offset);
    nodeID_t docNodeID{INVALID_OFFSET, internalTableInfo.docTable->getTableID()};
    internalTableInfo.docTable->lookupPK(transaction, &pkVector, 0 /* vectorPos */,
        docNodeID.offset);
    deleteState.updateVectors.idVector.setValue(0, docNodeID);
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
    ftsDeleteState.updateVectors.dstIDVector.setValue(0, docID);
    RelTableScanState relScanState{*transaction->getClientContext()->getMemoryManager(),
        &ftsDeleteState.updateVectors.srcIDVector,
        std::vector{
            &ftsDeleteState.updateVectors.idVector,
        },
        ftsDeleteState.updateVectors.dataChunkState, false /*randomLookup*/};

    //    const auto tableData = getDirectedTableData(direction);
    //    const auto reverseTableData =
    //        directedRelData.size() == NUM_REL_DIRECTIONS ?
    //            getDirectedTableData(RelDirectionUtils::getOppositeDirection(direction)) :
    //            nullptr;
    //    auto relReadState = std::make_unique<RelTableScanState>(
    //        *transaction->getClientContext()->getMemoryManager(), &deleteState->srcNodeIDVector,
    //        std::vector{&deleteState->dstNodeIDVector, &deleteState->relIDVector},
    //        deleteState->dstNodeIDVector.state, true /*randomLookup*/);
    //    relReadState->setToTable(transaction, this, {NBR_ID_COLUMN_ID, REL_ID_COLUMN_ID}, {},
    //        direction);
    //    initScanState(transaction, *relReadState);
    //    detachDeleteForCSRRels(transaction, tableData, reverseTableData, relReadState.get(),
    //        deleteState);
    //    if (transaction->shouldLogToWAL()) {
    //        KU_ASSERT(transaction->isWriteTransaction());
    //        KU_ASSERT(transaction->getClientContext());
    //        auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
    //        wal.logRelDetachDelete(tableID, direction, &deleteState->srcNodeIDVector);
    //    }
    //    hasChanges = true;
}

} // namespace fts_extension
} // namespace kuzu
