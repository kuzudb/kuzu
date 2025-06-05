#include "index/fts_index.h"

#include "catalog/fts_index_catalog_entry.h"
#include "index/fts_internal_table_info.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::storage;
using namespace kuzu::common;
using namespace kuzu::transaction;

FTSIndex::FTSIndex(IndexInfo indexInfo, FTSConfig config, main::ClientContext* context)
    : Index{indexInfo, std::make_unique<IndexStorageInfo>()},
      internalTableInfo{context, indexInfo.tableID, indexInfo.name, config.stopWordsTableName},
      config{std::move(config)}, context{context} {}

std::unique_ptr<Index> FTSIndex::load(main::ClientContext* context,
    storage::StorageManager* /*storageManager*/, storage::IndexInfo indexInfo,
    std::span<uint8_t> /*storageInfoBuffer*/) {
    auto catalog = context->getCatalog();
    auto indexEntry =
        catalog->getIndex(&transaction::DUMMY_TRANSACTION, indexInfo.tableID, indexInfo.name);
    auto ftsConfig = indexEntry->getAuxInfo().cast<FTSIndexAuxInfo>().config;
    return std::make_unique<FTSIndex>(std::move(indexInfo), std::move(ftsConfig), context);
}

struct FTSInsertState : public storage::Index::InsertState {
    MemoryManager* mm;
    std::shared_ptr<DataChunkState> dataChunkState;
    ValueVector idVector;
    ValueVector srcIDVector;
    ValueVector dstIDVector;
    ValueVector int64PKVector;
    ValueVector stringPKVector;
    ValueVector uint64PropVector;

    NodeTableInsertState docTableInsertState;
    NodeTableScanState termsTableScanState;
    NodeTableUpdateState termsTableUpdateState;
    NodeTableInsertState termsTableInsertState;
    RelTableInsertState appearsInTableInsertState;

    FTSInsertState(MemoryManager* mm, column_id_t dfColumnID, const Transaction* transaction,
        FTSInternalTableInfo& tableInfo);
};

FTSInsertState::FTSInsertState(MemoryManager* mm, column_id_t dfColumnID,
    const Transaction* transaction, FTSInternalTableInfo& tableInfo)
    : mm{mm}, dataChunkState{DataChunkState::getSingleValueDataChunkState()},
      idVector{LogicalType::INTERNAL_ID(), mm, dataChunkState},
      srcIDVector{LogicalType::INTERNAL_ID(), mm, dataChunkState},
      dstIDVector{LogicalType::INTERNAL_ID(), mm, dataChunkState},
      int64PKVector{LogicalType::INT64(), mm, dataChunkState},
      stringPKVector{LogicalType::STRING(), mm, dataChunkState},
      uint64PropVector{LogicalType::UINT64(), mm, dataChunkState},
      docTableInsertState{idVector, int64PKVector,
          std::vector<common::ValueVector*>{&int64PKVector, &uint64PropVector}},
      termsTableScanState{&idVector, std::vector{&uint64PropVector}, dataChunkState},
      termsTableUpdateState{dfColumnID, idVector, uint64PropVector},
      termsTableInsertState{idVector, stringPKVector,
          std::vector<common::ValueVector*>{&stringPKVector, &uint64PropVector}},
      appearsInTableInsertState{srcIDVector, dstIDVector, {&idVector, &uint64PropVector}} {
    termsTableScanState.setToTable(transaction, tableInfo.termsTable, {tableInfo.dfColumnID}, {});
    tableInfo.docTable->initInsertState(transaction, docTableInsertState);
    tableInfo.termsTable->initInsertState(transaction, termsTableInsertState);
    tableInfo.appearsInfoTable->initInsertState(transaction, appearsInTableInsertState);
}

struct TermInfo {
    common::offset_t offset;
    uint64_t tf;
};

std::unique_ptr<Index::InsertState> FTSIndex::initInsertState(
    const transaction::Transaction* transaction, MemoryManager* mm, visible_func /*isVisible*/) {
    return std::make_unique<FTSInsertState>(mm, internalTableInfo.dfColumnID, transaction,
        internalTableInfo);
}

static std::vector<std::string> getTerms(Transaction* transaction, FTSConfig& config,
    NodeTable* stopWordsTable, const std::vector<common::ValueVector*>& indexVectors, sel_t pos,
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
            transaction, false /* isConjunctive */);
        terms.insert(terms.end(), termsInContent.begin(), termsInContent.end());
    }
    return terms;
}

void FTSIndex::insert(Transaction* transaction, const common::ValueVector& nodeIDVector,
    const std::vector<common::ValueVector*>& indexVectors, Index::InsertState& insertState) {
    auto totalInsertedDocLen = 0u;
    for (auto i = 0u; i < nodeIDVector.state->getSelSize(); i++) {
        auto& ftsInsertState = insertState.cast<FTSInsertState>();
        auto pos = nodeIDVector.state->getSelVector()[i];
        auto insertedNodeID = nodeIDVector.getValue<nodeID_t>(pos);
        auto terms = getTerms(transaction, config, internalTableInfo.stopWordsTable, indexVectors,
            pos, ftsInsertState.mm);
        totalInsertedDocLen += terms.size();
        std::unordered_map<std::string, TermInfo> tfCollection;
        for (auto& term : terms) {
            tfCollection[term].tf++;
        }
        auto insertedDocID = insertToDocTable(transaction, ftsInsertState, insertedNodeID, terms);
        updateTermsTable(transaction, tfCollection, ftsInsertState);
        insertToAppearsInTable(transaction, tfCollection, ftsInsertState, insertedDocID,
            internalTableInfo.termsTable->getTableID());
    }
    auto& ftsAuxInfo = context->getCatalog()
                           ->getIndex(context->getTransaction(), indexInfo.tableID, indexInfo.name)
                           ->getAuxInfoUnsafe()
                           .cast<FTSIndexAuxInfo>();
    auto numInsertedDocs = nodeIDVector.state->getSelSize();
    ftsAuxInfo.avgDocLen = (ftsAuxInfo.avgDocLen * ftsAuxInfo.numDocs + totalInsertedDocLen) /
                           (ftsAuxInfo.numDocs + numInsertedDocs);
    ftsAuxInfo.numDocs += numInsertedDocs;
}

nodeID_t FTSIndex::insertToDocTable(Transaction* transaction, FTSInsertState& insertState,
    common::nodeID_t insertedNodeID, const std::vector<std::string>& terms) const {
    auto& ftsInsertState = insertState.cast<FTSInsertState>();
    ftsInsertState.int64PKVector.setValue(0, insertedNodeID.offset);
    ftsInsertState.uint64PropVector.setValue(0, (uint64_t)terms.size());
    auto docTable = internalTableInfo.docTable;
    docTable->insert(transaction, ftsInsertState.docTableInsertState);
    return ftsInsertState.idVector.getValue<nodeID_t>(
        ftsInsertState.idVector.state->getSelVector()[0]);
}

void FTSIndex::updateTermsTable(Transaction* transaction,
    std::unordered_map<std::string, TermInfo>& tfCollection, FTSInsertState& ftsInsertState) const {
    auto termsTable = internalTableInfo.termsTable;
    internalID_t termNodeID{INVALID_OFFSET, termsTable->getTableID()};
    // Insert to terms table
    for (auto& [term, termInfo] : tfCollection) {
        ftsInsertState.stringPKVector.setValue(0, term);
        // If the word already exists in the dict table, we update the df. Otherwise, we
        // insert a new word entry to the dict table.
        if (termsTable->lookupPK(transaction, &ftsInsertState.stringPKVector, 0 /* vectorPos */,
                termNodeID.offset)) {
            ftsInsertState.idVector.setValue(0, termNodeID);
            termsTable->initScanState(transaction, ftsInsertState.termsTableScanState,
                termNodeID.tableID, termNodeID.offset);
            termsTable->lookup(transaction, ftsInsertState.termsTableScanState);
            ftsInsertState.uint64PropVector.setValue(0,
                ftsInsertState.uint64PropVector.getValue<uint64_t>(0) + 1);
            termsTable->update(transaction, ftsInsertState.termsTableUpdateState);
            termInfo.offset = termNodeID.offset;
        } else {
            ftsInsertState.uint64PropVector.setValue(0, 1);
            termsTable->insert(transaction, ftsInsertState.termsTableInsertState);
            termInfo.offset = ftsInsertState.idVector.getValue<nodeID_t>(0).offset;
        }
    }
}

void FTSIndex::insertToAppearsInTable(Transaction* transaction,
    const std::unordered_map<std::string, TermInfo>& tfCollection, FTSInsertState& ftsInsertState,
    nodeID_t docID, table_id_t termsTableID) const {
    ftsInsertState.dstIDVector.setValue(0, docID);
    for (auto& [term, termInfo] : tfCollection) {
        ftsInsertState.srcIDVector.setValue(0, nodeID_t{termInfo.offset, termsTableID});
        ftsInsertState.uint64PropVector.setValue(0, termInfo.tf);
        internalTableInfo.appearsInfoTable->insert(transaction,
            ftsInsertState.appearsInTableInsertState);
    }
}

} // namespace fts_extension
} // namespace kuzu
