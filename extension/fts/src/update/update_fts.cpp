#include "update/update_fts.h"

#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/string_utils.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::storage;

FTSUpdater::FTSUpdater(catalog::IndexCatalogEntry* ftsEntry, main::ClientContext* context)
    : ftsEntry{ftsEntry}, dataChunkState{DataChunkState::getSingleValueDataChunkState()},
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
    auto indexName = ftsEntry->getIndexName();
    auto docTableName = FTSUtils::getDocsTableName(ftsEntry->getTableID(), indexName);
    auto termsTableName = FTSUtils::getTermsTableName(ftsEntry->getTableID(), indexName);
    auto appearsInTableName = FTSUtils::getAppearsInTableName(ftsEntry->getTableID(), indexName);
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

struct TermInfo {
    common::offset_t offset;
    uint64_t tf;
};

void FTSUpdater::insertNode(main::ClientContext* context, common::nodeID_t insertedNodeID,
    std::vector<common::ValueVector*> columnDataVectors) {
    std::string content;
    std::vector<std::string> terms;
    for (auto dataVectorIdx : dataVectorIdxes) {
        auto columnDataVector = columnDataVectors[dataVectorIdx];
        KU_ASSERT(columnDataVector->state->isFlat());
        auto pos = columnDataVector->state->getSelVector()[0];
        if (columnDataVector->isNull(pos)) {
            continue;
        }
        content = columnDataVector->getValue<ku_string_t>(pos).getAsString();
        FTSUtils::normalizeQuery(content);
        auto termsInContent = StringUtils::split(content, " ");
        termsInContent = FTSUtils::stemTerms(termsInContent,
            ftsEntry->getAuxInfo().cast<FTSIndexAuxInfo>().config, *context,
            false /* isConjunctive */);
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

void FTSUpdater::deleteNode(main::ClientContext* context, common::nodeID_t deletedNodeID) {
    // Delete from doc table
    docPKVector.setValue(0, deletedNodeID.offset);
    nodeID_t docNodeID{INVALID_OFFSET, docTable->getTableID()};
    docTable->lookupPK(context->getTransaction(), &docPKVector, 0 /* vectorPos */,
        docNodeID.offset);
    docNodeIDVector.setValue(0, docNodeID);
    auto nodeDeleteState = std::make_unique<NodeTableDeleteState>(docNodeIDVector, docPKVector);
    docTable->delete_(context->getTransaction(), *nodeDeleteState);

    // Update dict table
    std::unordered_map<std::string, offset_t> terms;
    nodeID_t termNodeID{INVALID_OFFSET, termsTable->getTableID()};
    for (auto& [term, offset] : terms) {
        termsPKVector.setValue(0, term);
        // If the word already exists in the dict table, we update the df. Otherwise, we
        // insert a new word entry to the dict table.
        auto found = termsTable->lookupPK(context->getTransaction(), &termsPKVector,
            0 /* vectorPos */, offset);
        termNodeID.offset = offset;
        KU_ASSERT(found);
        auto nodeTableScanState =
            NodeTableScanState(&termsNodeIDVector, std::vector{&termsDFVector}, dataChunkState);
        nodeTableScanState.setToTable(context->getTransaction(), termsTable, {dfColumnID}, {});
        termsNodeIDVector.setValue(0, termNodeID);
        termsTable->initScanState(context->getTransaction(), nodeTableScanState, termNodeID.tableID,
            termNodeID.offset);
        termsTable->lookup(context->getTransaction(), nodeTableScanState);
        auto df = termsDFVector.getValue<uint64_t>(0);
        if (df == 1) {
            // Delete from terms table
            nodeDeleteState =
                std::make_unique<NodeTableDeleteState>(termsNodeIDVector, termsPKVector);
            docTable->delete_(context->getTransaction(), *nodeDeleteState);
        } else {
            // Update terms table
            NodeTableUpdateState updateState{dfColumnID, termsNodeIDVector, termsDFVector};
            termsDFVector.setValue(0, df - 1);
            termsTable->update(context->getTransaction(), updateState);
        }
    }

    // Delete from appearsInfo table
    auto deleteState =
        std::make_unique<RelTableDeleteState>(appearsInSrcVector, appearsInDstVector, relIDVector);
    relID_t relID = {INVALID_OFFSET, appearsInTable->getTableID()};

    // 1， alice, 5
    //1， alice, 3
    // 1, bob, 2
    // 1, bob, 1
    for (auto& [_, offset] : terms) {
        termNodeID.offset = offset;
        appearsInSrcVector.setValue(0, termNodeID);
        appearsInDstVector.setValue(0, docNodeID);
        relIDVector.setValue(0, relID);
        termsTable->delete_(context->getTransaction(), *deleteState);
    }
}

} // namespace fts_extension
} // namespace kuzu
