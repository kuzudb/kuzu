#include "processor/operator/semi_masker.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void BaseSemiMasker::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    keyValueVector = resultSet->getValueVector(keyDataPos);
    assert(keyValueVector->dataType.typeID == INTERNAL_ID);
}

static mask_and_idx_pair initSemiMaskForTableState(
    NodeTableState* tableState, transaction::Transaction* trx) {
    tableState->initSemiMask(trx);
    auto maskerIdx = tableState->getNumMaskers();
    assert(maskerIdx < UINT8_MAX);
    tableState->incrementNumMaskers();
    return std::make_pair(tableState->getSemiMask(), maskerIdx);
}

void SingleTableSemiMasker::initGlobalStateInternal(kuzu::processor::ExecutionContext* context) {
    for (auto& scanState : scanStates) {
        assert(scanState->getNumTableStates() == 1);
        auto tableState = scanState->getTableState(0);
        maskPerScan.push_back(initSemiMaskForTableState(tableState, context->transaction));
    }
}

bool SingleTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto numValues =
        keyValueVector->state->isFlat() ? 1 : keyValueVector->state->selVector->selectedSize;
    for (auto [mask, maskerIdx] : maskPerScan) {
        for (auto i = 0u; i < numValues; i++) {
            auto pos = keyValueVector->state->selVector->selectedPositions[i];
            auto nodeID = keyValueVector->getValue<nodeID_t>(pos);
            mask->incrementMaskValue(nodeID.offset, maskerIdx);
        }
    }
    metrics->numOutputTuple.increase(numValues);
    return true;
}

void MultiTableSemiMasker::initGlobalStateInternal(kuzu::processor::ExecutionContext* context) {
    for (auto& scanState : scanStates) {
        assert(scanState->getNumTableStates() > 1);
        std::unordered_map<common::table_id_t, mask_and_idx_pair> maskerPerLabel;
        for (auto i = 0u; i < scanState->getNumTableStates(); ++i) {
            auto tableState = scanState->getTableState(i);
            maskerPerLabel.insert({tableState->getTable()->getTableID(),
                initSemiMaskForTableState(tableState, context->transaction)});
        }
        maskerPerLabelPerScan.push_back(std::move(maskerPerLabel));
    }
}

bool MultiTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto numValues =
        keyValueVector->state->isFlat() ? 1 : keyValueVector->state->selVector->selectedSize;
    for (auto& maskerPerLabel : maskerPerLabelPerScan) {
        for (auto i = 0u; i < numValues; i++) {
            auto pos = keyValueVector->state->selVector->selectedPositions[i];
            auto nodeID = keyValueVector->getValue<nodeID_t>(pos);
            auto [mask, maskerIdx] = maskerPerLabel.at(nodeID.tableID);
            mask->incrementMaskValue(nodeID.offset, maskerIdx);
        }
    }
    metrics->numOutputTuple.increase(numValues);
    return true;
}

} // namespace processor
} // namespace kuzu
