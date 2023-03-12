#include "processor/operator/semi_masker.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void BaseSemiMasker::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    keyValueVector = resultSet->getValueVector(keyDataPos);
    assert(keyValueVector->dataType.typeID == INTERNAL_ID);
}

static std::pair<uint8_t, NodeTableSemiMask*> initSemiMaskForTableState(
    NodeTableState* tableState, transaction::Transaction* trx) {
    tableState->initSemiMask(trx);
    auto maskerIdx = tableState->getNumMaskers();
    assert(maskerIdx < UINT8_MAX);
    tableState->incrementNumMaskers();
    return std::make_pair(maskerIdx, tableState->getSemiMask());
}

void SingleTableSemiMasker::initGlobalStateInternal(kuzu::processor::ExecutionContext* context) {
    assert(scanNodeIDSharedState->getNumTableStates() == 1);
    auto tableState = scanNodeIDSharedState->getTableState(0);
    maskerIdxAndMask = initSemiMaskForTableState(tableState, context->transaction);
}

bool SingleTableSemiMasker::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    auto [maskerIdx, mask] = maskerIdxAndMask;
    auto numValues =
        keyValueVector->state->isFlat() ? 1 : keyValueVector->state->selVector->selectedSize;
    for (auto i = 0u; i < numValues; i++) {
        auto pos = keyValueVector->state->selVector->selectedPositions[i];
        auto nodeID = keyValueVector->getValue<nodeID_t>(pos);
        mask->incrementMaskValue(nodeID.offset, maskerIdx);
    }
    metrics->numOutputTuple.increase(numValues);
    return true;
}

void MultiTableSemiMasker::initGlobalStateInternal(kuzu::processor::ExecutionContext* context) {
    assert(scanNodeIDSharedState->getNumTableStates() > 1);
    for (auto i = 0u; i < scanNodeIDSharedState->getNumTableStates(); ++i) {
        auto tableState = scanNodeIDSharedState->getTableState(i);
        auto maskerIdxAndMask = initSemiMaskForTableState(tableState, context->transaction);
        maskerIdxAndMasks.insert(
            {tableState->getTable()->getTableID(), std::move(maskerIdxAndMask)});
    }
}

bool MultiTableSemiMasker::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    auto numValues =
        keyValueVector->state->isFlat() ? 1 : keyValueVector->state->selVector->selectedSize;
    for (auto i = 0u; i < numValues; i++) {
        auto pos = keyValueVector->state->selVector->selectedPositions[i];
        auto nodeID = keyValueVector->getValue<nodeID_t>(pos);
        auto [maskerIdx, mask] = maskerIdxAndMasks.at(nodeID.tableID);
        mask->incrementMaskValue(nodeID.offset, maskerIdx);
    }
    metrics->numOutputTuple.increase(numValues);
    return true;
}

} // namespace processor
} // namespace kuzu
