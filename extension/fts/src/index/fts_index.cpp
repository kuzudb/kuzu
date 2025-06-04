#include "index/fts_index.h"

#include "catalog/fts_index_catalog_entry.h"
#include "index/fts_storage_info.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::storage;
using namespace kuzu::common;
using namespace kuzu::transaction;

FTSIndex::FTSIndex(IndexInfo indexInfo, std::unique_ptr<IndexStorageInfo> storageInfo,
    FTSConfig config)
    : Index{std::move(indexInfo), std::move(storageInfo)}, config{std::move(config)} {}

std::unique_ptr<Index> FTSIndex::load(main::ClientContext* context,
    storage::StorageManager* /*storageManager*/, storage::IndexInfo indexInfo,
    std::span<uint8_t> /*storageInfoBuffer*/) {
    auto catalog = context->getCatalog();
    auto indexEntry =
        catalog->getIndex(&transaction::DUMMY_TRANSACTION, indexInfo.tableID, indexInfo.name);
    auto ftsConfig = indexEntry->getAuxInfo().cast<FTSIndexAuxInfo>().config;
    auto ftsStorageInfo = std::make_unique<FTSStorageInfo>(context, indexInfo.tableID,
        indexInfo.name, ftsConfig.stopWordsTableName);
    return std::make_unique<FTSIndex>(std::move(indexInfo), std::move(ftsStorageInfo),
        std::move(ftsConfig));
}

struct FTSInsertState : public storage::Index::InsertState {
    storage::MemoryManager* mm;
    std::shared_ptr<common::DataChunkState> dataChunkState;
    // Doc table
    common::ValueVector docNodeIDVector;
    common::ValueVector docPKVector;
    common::ValueVector lenVector;
    std::vector<common::ValueVector*> docProperties;
    // Terms table
    common::ValueVector termsNodeIDVector;
    common::ValueVector termsPKVector;
    common::ValueVector termsDFVector;
    std::vector<common::ValueVector*> termsProperties;
    // AppearsIn table
    common::ValueVector appearsInSrcVector;
    common::ValueVector appearsInDstVector;
    common::ValueVector relIDVector;
    common::ValueVector tfVector;
    std::vector<common::ValueVector*> appearsInProperties;

    explicit FTSInsertState(MemoryManager* mm);
};

FTSInsertState::FTSInsertState(MemoryManager* mm)
    : mm{mm}, dataChunkState{DataChunkState::getSingleValueDataChunkState()},
      docNodeIDVector{LogicalType::INTERNAL_ID(), mm, dataChunkState},
      docPKVector{LogicalType::INT64(), mm, dataChunkState},
      lenVector{LogicalType::UINT64(), mm, dataChunkState},
      termsNodeIDVector{LogicalType::INTERNAL_ID(), mm, dataChunkState},
      termsPKVector{LogicalType::STRING(), mm, dataChunkState},
      termsDFVector{LogicalType::UINT64(), mm, dataChunkState},
      appearsInSrcVector{LogicalType::INTERNAL_ID(), mm, dataChunkState},
      appearsInDstVector{LogicalType::INTERNAL_ID(), mm, dataChunkState},
      relIDVector{LogicalType::INTERNAL_ID(), mm, dataChunkState},
      tfVector{LogicalType::UINT64(), mm, dataChunkState} {
    docProperties.push_back(&docPKVector);
    docProperties.push_back(&lenVector);
    termsProperties.push_back(&termsPKVector);
    termsProperties.push_back(&termsDFVector);
    appearsInProperties.push_back(&relIDVector);
    appearsInProperties.push_back(&tfVector);
}

std::unique_ptr<Index::InsertState> FTSIndex::initInsertState(
    const transaction::Transaction* /*transaction*/, MemoryManager* mm,
    visible_func /*isVisible*/) {
    return std::make_unique<FTSInsertState>(mm);
}

struct TermInfo {
    common::offset_t offset;
    uint64_t tf;
};

void FTSIndex::insert(Transaction* transaction, const common::ValueVector& nodeIDVector,
    const std::vector<common::ValueVector*>& indexVectors, Index::InsertState& insertState) {
    for (auto i = 0u; i < nodeIDVector.state->getSelSize(); i++) {
        auto pos = nodeIDVector.state->getSelVector()[i];
        auto insertedNodeID = nodeIDVector.getValue<nodeID_t>(pos);
        std::string content;
        std::vector<std::string> terms;
        auto& ftsInsertState = insertState.cast<FTSInsertState>();
        auto& ftsStorageInfo = storageInfo->cast<FTSStorageInfo>();
        for (auto indexVector : indexVectors) {
            if (indexVector->isNull(pos)) {
                continue;
            }
            content = indexVector->getValue<ku_string_t>(pos).getAsString();
            FTSUtils::normalizeQuery(content);
            auto termsInContent = StringUtils::split(content, " ");
            termsInContent = FTSUtils::stemTerms(termsInContent, config, ftsInsertState.mm,
                ftsStorageInfo.stopWordsTable, transaction, false /* isConjunctive */);
            terms.insert(terms.end(), termsInContent.begin(), termsInContent.end());
        }

        ftsInsertState.docPKVector.setValue(0, insertedNodeID.offset);
        ftsInsertState.lenVector.setValue(0, (uint64_t)terms.size());
        // Insert to doc table
        auto docTable = ftsStorageInfo.docTable;
        auto termsTable = ftsStorageInfo.termsTable;
        auto appearsInTable = ftsStorageInfo.appearsInfoTable;
        auto nodeInsertState =
            std::make_unique<NodeTableInsertState>(ftsInsertState.docNodeIDVector,
                ftsInsertState.docPKVector, ftsInsertState.docProperties);
        docTable->initInsertState(transaction, *nodeInsertState);
        docTable->insert(transaction, *nodeInsertState);

        std::unordered_map<std::string, TermInfo> tfCollection;
        for (auto& term : terms) {
            tfCollection[term].tf++;
        }

        internalID_t termNodeID{INVALID_OFFSET, termsTable->getTableID()};
        NodeTableUpdateState updateState{ftsStorageInfo.dfColumnID,
            ftsInsertState.termsNodeIDVector, ftsInsertState.termsDFVector};

        auto nodeTableScanState = NodeTableScanState(&ftsInsertState.termsNodeIDVector,
            std::vector{&ftsInsertState.termsDFVector}, ftsInsertState.dataChunkState);
        nodeTableScanState.setToTable(transaction, termsTable, {ftsStorageInfo.dfColumnID}, {});
        // Insert to dict table
        for (auto& [term, termInfo] : tfCollection) {
            ftsInsertState.termsPKVector.setValue(0, term);
            // If the word already exists in the dict table, we update the df. Otherwise, we
            // insert a new word entry to the dict table.
            if (termsTable->lookupPK(transaction, &ftsInsertState.termsPKVector, 0 /* vectorPos */,
                    termNodeID.offset)) {
                ftsInsertState.termsNodeIDVector.setValue(0, termNodeID);
                termsTable->initScanState(transaction, nodeTableScanState, termNodeID.tableID,
                    termNodeID.offset);
                termsTable->lookup(transaction, nodeTableScanState);
                ftsInsertState.termsDFVector.setValue(0,
                    ftsInsertState.termsDFVector.getValue<uint64_t>(0) + 1);
                termsTable->update(transaction, updateState);
                termInfo.offset = termNodeID.offset;
            } else {
                ftsInsertState.termsDFVector.setValue(0, 1);
                nodeInsertState =
                    std::make_unique<NodeTableInsertState>(ftsInsertState.termsNodeIDVector,
                        ftsInsertState.termsPKVector, ftsInsertState.termsProperties);
                termsTable->initInsertState(transaction, *nodeInsertState);
                termsTable->insert(transaction, *nodeInsertState);
                termInfo.offset = ftsInsertState.termsNodeIDVector.getValue<nodeID_t>(0).offset;
            }
        }

        // Insert to appears in table
        auto relInsertState =
            std::make_unique<storage::RelTableInsertState>(ftsInsertState.appearsInSrcVector,
                ftsInsertState.appearsInDstVector, ftsInsertState.appearsInProperties);
        appearsInTable->initInsertState(transaction, *relInsertState);
        ftsInsertState.appearsInDstVector.setValue(0,
            ftsInsertState.docNodeIDVector.getValue<nodeID_t>(0));
        for (auto& [term, termInfo] : tfCollection) {
            ftsInsertState.appearsInSrcVector.setValue(0,
                nodeID_t{termInfo.offset, termsTable->getTableID()});
            ftsInsertState.tfVector.setValue(0, termInfo.tf);
            appearsInTable->insert(transaction, *relInsertState);
        }
    }
}

} // namespace fts_extension
} // namespace kuzu
