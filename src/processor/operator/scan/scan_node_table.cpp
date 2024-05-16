#include "processor/operator/scan/scan_node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void ScanNodeTableSharedState::initialize(const transaction::Transaction* transaction,
    storage::NodeTable* table_) {
    this->table = table_;
    this->currentGroupIdx = 0;
    this->numNodeGroups = table->getNumNodeGroups(transaction);
}

node_group_idx_t ScanNodeTableSharedState::getNextMorsel() {
    std::unique_lock lck{mtx};
    if (currentGroupIdx == numNodeGroups) {
        return INVALID_NODE_GROUP_IDX;
    }
    return currentGroupIdx++;
}

void ScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (const auto& info : infos) {
        auto scanState = std::make_unique<storage::NodeTableReadState>(info->columnIDs);
        initVectors(*scanState, *resultSet);
        scanStates.push_back(std::move(scanState));
    }
}

void ScanNodeTable::initGlobalStateInternal(ExecutionContext* context) {
    KU_ASSERT(sharedStates.size() == infos.size());
    for (auto i = 0u; i < infos.size(); i++) {
        sharedStates[i]->initialize(context->clientContext->getTx(), infos[i]->table);
    }
}

bool ScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    while (currentTableIdx < infos.size()) {
        const auto info = infos[currentTableIdx].get();
        const auto readState = scanStates[currentTableIdx].get();
        while (true) {
            if (readState->hasMoreToRead(context->clientContext->getTx())) {
                info->table->scan(context->clientContext->getTx(), *readState);
                return true;
            }
            const auto nodeGroupIdx = sharedStates[currentTableIdx]->getNextMorsel();
            if (nodeGroupIdx == INVALID_NODE_GROUP_IDX) {
                // Current table out of data, move to the next one.
                currentTableIdx++;
                break;
            }
            info->table->initializeScanState(context->clientContext->getTx(), nodeGroupIdx,
                info->columnIDs, *readState);
            info->table->scan(context->clientContext->getTx(), *readState);
            if (readState->nodeIDVector->state->getSelVector().getSelSize() > 0) {
                return true;
            }
        }
    }
    return false;
}

std::unique_ptr<PhysicalOperator> ScanNodeTable::clone() {
    std::vector<std::unique_ptr<ScanNodeTableInfo>> clonedInfos;
    for (const auto& info : infos) {
        clonedInfos.push_back(info->copy());
    }
    return make_unique<ScanNodeTable>(nodeIDPos, outVectorsPos, std::move(clonedInfos),
        sharedStates, id, paramsString);
}

} // namespace processor
} // namespace kuzu
