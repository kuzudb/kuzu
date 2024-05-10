#include "processor/operator/scan/scan_node_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

// void ScanNodeTableSharedState::initialize(const Transaction* transaction,
//     storage::NodeTable* table) {
//     this->table = table;
//     this->currentGroupIdx = 0;
//     this->numNodeGroups = table->getNumNodeGroups(transaction);
// }
//
// node_group_idx_t ScanNodeTableSharedState::getNextMorsel() {
//     std::unique_lock lck{mtx};
//     if (currentGroupIdx == numNodeGroups) {
//         return INVALID_NODE_GROUP_IDX;
//     }
//     return currentGroupIdx++;
// }
//
// void ScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
//     ScanTable::initLocalStateInternal(resultSet, context);
//     readState = std::make_unique<storage::NodeTableReadState>(info->columnIDs);
//     initVectors(*readState, *resultSet);
// }
//
// bool ScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
//     do {
//         if (readState->hasMoreToRead(context->clientContext->getTx())) {
//             info->table->read(context->clientContext->getTx(), *readState);
//             return true;
//         }
//         const auto nodeGroupIdx = sharedState->getNextMorsel();
//         if (nodeGroupIdx == INVALID_NODE_GROUP_IDX) {
//             return false;
//         }
//         info->table->initializeReadState(context->clientContext->getTx(), nodeGroupIdx,
//             info->columnIDs, *readState);
//         info->table->read(context->clientContext->getTx(), *readState);
//     } while (readState->nodeIDVector->state->getSelVector().getSelSize() == 0);
//     return true;
// }

} // namespace processor
} // namespace kuzu
