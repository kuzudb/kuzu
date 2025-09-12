#include "function/query_fts/query_fts_term_lookup.h"

#include "storage/storage_manager.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::main;
using namespace kuzu::storage;
using namespace kuzu::transaction;

TermsDFLookup::TermsDFLookup(TableCatalogEntry* termsEntry, ClientContext& context)
    : dataChunkState{DataChunkState::getSingleValueDataChunkState()},
      termsVector{LogicalType::STRING(), MemoryManager::Get(context)},
      nodeIDVector{LogicalType::INTERNAL_ID()}, dfVector{LogicalType::UINT64()},
      termsTable{
          StorageManager::Get(context)->getTable(termsEntry->getTableID())->cast<NodeTable>()},
      nodeTableScanState{&nodeIDVector, std::vector{&dfVector}, dataChunkState},
      dfColumnID{termsEntry->getColumnID(DOC_FREQUENCY_PROP_NAME)}, trx{Transaction::Get(context)} {
    termsVector.state = dataChunkState;
    nodeIDVector.state = dataChunkState;
    dfVector.state = dataChunkState;
    nodeTableScanState.setToTable(transaction::Transaction::Get(context), &termsTable, {dfColumnID},
        {});
}

std::pair<offset_t, uint64_t> TermsDFLookup::lookupTermDF(const std::string& term) {
    termsVector.setValue(0, term);
    offset_t offset = 0;
    if (!termsTable.lookupPK(trx, &termsVector, 0 /* vectorPos */, offset)) {
        return {INVALID_OFFSET, UINT64_MAX};
    }
    auto nodeID = nodeID_t{offset, termsTable.getTableID()};
    nodeIDVector.setValue(0, nodeID);
    termsTable.initScanState(trx, nodeTableScanState, termsTable.getTableID(), offset);
    termsTable.lookup(trx, nodeTableScanState);
    return {offset, dfVector.getValue<uint64_t>(0)};
}

} // namespace fts_extension
} // namespace kuzu
