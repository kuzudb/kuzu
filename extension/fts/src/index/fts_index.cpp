#include "index/fts_index.h"

#include "common/types/types.h"
#include "storage/storage_manager.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::storage;
using namespace kuzu::common;

FTSIndex::FTSIndex(IndexInfo indexInfo, std::unique_ptr<IndexStorageInfo> storageInfo,
    FTSConfig config, main::ClientContext* context)
    : Index{std::move(indexInfo), std::move(storageInfo)}, config{std::move(config)},
      dataChunkState{DataChunkState::getSingleValueDataChunkState()},
      docNodeIDVector{LogicalType::INTERNAL_ID(), context->getMemoryManager(), dataChunkState},
      docPKVector{LogicalType::INT64(), context->getMemoryManager(), dataChunkState},
      lenVector{LogicalType::UINT64(), context->getMemoryManager(), dataChunkState},
      termsNodeIDVector{LogicalType::INTERNAL_ID(), context->getMemoryManager(), dataChunkState},
      termsPKVector{LogicalType::STRING(), context->getMemoryManager(), dataChunkState},
      termsDFVector{LogicalType::UINT64(), context->getMemoryManager(), dataChunkState},
      appearsInSrcVector{LogicalType::INTERNAL_ID(), context->getMemoryManager(), dataChunkState},
      appearsInDstVector{LogicalType::INTERNAL_ID(), context->getMemoryManager(), dataChunkState},
      relIDVector{LogicalType::INTERNAL_ID(), context->getMemoryManager(), dataChunkState},
      tfVector{LogicalType::UINT64(), context->getMemoryManager(), dataChunkState} {
    docProperties.push_back(&docPKVector);
    docProperties.push_back(&lenVector);
    termsProperties.push_back(&termsPKVector);
    termsProperties.push_back(&termsDFVector);
    appearsInProperties.push_back(&relIDVector);
    appearsInProperties.push_back(&tfVector);
    auto storageManager = context->getStorageManager();
    auto catalog = context->getCatalog();
    auto trx = context->getTransaction();
    auto indexName = indexInfo.name;
    auto tableID = indexInfo.tableID;
    auto docTableName = FTSUtils::getDocsTableName(tableID, indexName);
    auto termsTableName = FTSUtils::getTermsTableName(tableID, indexName);
    auto appearsInTableName = FTSUtils::getAppearsInTableName(tableID, indexName);
    docTable =
        storageManager->getTable(catalog->getTableCatalogEntry(trx, docTableName)->getTableID())
            ->ptrCast<NodeTable>();
    termsTable =
        storageManager->getTable(catalog->getTableCatalogEntry(trx, termsTableName)->getTableID())
            ->ptrCast<NodeTable>();
    auto appearsInTableEntry =
        catalog->getTableCatalogEntry(trx, appearsInTableName)
            ->constPtrCast<catalog::RelGroupCatalogEntry>()
            ->getRelEntryInfo(termsTable->getTableID(), docTable->getTableID());
    appearsInTable = storageManager->getTable(appearsInTableEntry->oid)->ptrCast<RelTable>();
    dfColumnID =
        catalog->getTableCatalogEntry(context->getTransaction(), termsTableName)->getColumnID("df");
}

std::unique_ptr<Index::InsertState> FTSIndex::initInsertState(
    const transaction::Transaction* /*transaction*/, MemoryManager* /*mm*/,
    visible_func /*isVisible*/) {
    return std::make_unique<Index::InsertState>();
}

struct TermInfo {
    common::offset_t offset;
    uint64_t tf;
};

void FTSIndex::insert(main::ClientContext* context, const common::ValueVector& nodeIDVector,
    const std::vector<common::ValueVector*>& indexVectors, Index::InsertState& insertState) {
    auto pos = nodeIDVector.state->getSelVector()[0];
    auto insertedNodeID = nodeIDVector.getValue<nodeID_t>(pos);
    std::string content;
    std::vector<std::string> terms;
    for (auto indexVector : indexVectors) {
        KU_ASSERT(indexVector->state->isFlat());
        if (indexVector->isNull(pos)) {
            continue;
        }
        content = indexVector->getValue<ku_string_t>(pos).getAsString();
        FTSUtils::normalizeQuery(content);
        auto termsInContent = StringUtils::split(content, " ");
        termsInContent =
            FTSUtils::stemTerms(termsInContent, config, *context, false /* isConjunctive */);
        terms.insert(terms.end(), termsInContent.begin(), termsInContent.end());
    }

    docPKVector.setValue(0, insertedNodeID.offset);
    lenVector.setValue(0, (uint64_t)terms.size());
    // Insert to doc table
    auto nodeInsertState =
        std::make_unique<NodeTableInsertState>(docNodeIDVector, docPKVector, docProperties);
    docTable->insert(context->getTransaction(), *nodeInsertState);

    std::unordered_map<std::string, TermInfo> tfCollection;
    for (auto& term : terms) {
        tfCollection[term].tf++;
    }

    internalID_t termNodeID{INVALID_OFFSET, termsTable->getTableID()};
    NodeTableUpdateState updateState{dfColumnID, termsNodeIDVector, termsDFVector};

    auto nodeTableScanState =
        NodeTableScanState(&termsNodeIDVector, std::vector{&termsDFVector}, dataChunkState);
    nodeTableScanState.setToTable(context->getTransaction(), termsTable, {dfColumnID}, {});
    // Insert to dict table
    for (auto& [term, termInfo] : tfCollection) {
        termsPKVector.setValue(0, term);
        // If the word already exists in the dict table, we update the df. Otherwise, we
        // insert a new word entry to the dict table.
        if (termsTable->lookupPK(context->getTransaction(), &termsPKVector, 0 /* vectorPos */,
                termNodeID.offset)) {
            termsNodeIDVector.setValue(0, termNodeID);
            termsTable->initScanState(context->getTransaction(), nodeTableScanState,
                termNodeID.tableID, termNodeID.offset);
            termsTable->lookup(context->getTransaction(), nodeTableScanState);
            termsDFVector.setValue(0, termsDFVector.getValue<uint64_t>(0) + 1);
            termsTable->update(context->getTransaction(), updateState);
            termInfo.offset = termNodeID.offset;
        } else {
            termsDFVector.setValue(0, 1);
            nodeInsertState = std::make_unique<NodeTableInsertState>(termsNodeIDVector,
                termsPKVector, termsProperties);
            termsTable->insert(context->getTransaction(), *nodeInsertState);
            termInfo.offset = termsNodeIDVector.getValue<nodeID_t>(0).offset;
        }
    }

    // Insert to appears in table
    auto relInsertState = std::make_unique<storage::RelTableInsertState>(appearsInSrcVector,
        appearsInDstVector, appearsInProperties);
    appearsInDstVector.setValue(0, docNodeIDVector.getValue<nodeID_t>(0));
    for (auto& [term, termInfo] : tfCollection) {
        appearsInSrcVector.setValue(0, nodeID_t{termInfo.offset, termsTable->getTableID()});
        tfVector.setValue(0, termInfo.tf);
        appearsInTable->insert(context->getTransaction(), *relInsertState);
    }
}

} // namespace fts_extension
} // namespace kuzu
