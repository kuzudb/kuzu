#include "processor/operator/semi_masker.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void BaseSemiMasker::initGlobalStateInternal(ExecutionContext* context) {
    for (auto& [table, masks] : masksPerTable) {
        for (auto& maskWithIdx : masks) {
            auto maskIdx = maskWithIdx.first->getNumMasks();
            assert(maskIdx < UINT8_MAX);
            maskWithIdx.first->incrementNumMasks();
            maskWithIdx.second = maskIdx;
        }
    }
}

void BaseSemiMasker::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    keyValueVector = resultSet->getValueVector(keyDataPos);
    assert(keyValueVector->dataType.typeID == INTERNAL_ID);
    for (auto& [table, masks] : masksPerTable) {
        for (auto& maskWithIdx : masks) {
            maskWithIdx.first->init(transaction);
        }
    }
}

bool SingleTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto numValues =
        keyValueVector->state->isFlat() ? 1 : keyValueVector->state->selVector->selectedSize;
    for (auto i = 0u; i < numValues; i++) {
        auto pos = keyValueVector->state->selVector->selectedPositions[i];
        auto nodeID = keyValueVector->getValue<nodeID_t>(pos);
        for (auto [mask, maskerIdx] : masksPerTable.begin()->second) {
            mask->incrementMaskValue(nodeID.offset, maskerIdx);
        }
    }
    metrics->numOutputTuple.increase(numValues);
    return true;
}

bool MultiTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto numValues =
        keyValueVector->state->isFlat() ? 1 : keyValueVector->state->selVector->selectedSize;
    for (auto i = 0u; i < numValues; i++) {
        auto pos = keyValueVector->state->selVector->selectedPositions[i];
        auto nodeID = keyValueVector->getValue<nodeID_t>(pos);
        for (auto [mask, maskerIdx] : masksPerTable.at(nodeID.tableID)) {
            mask->incrementMaskValue(nodeID.offset, maskerIdx);
        }
    }
    metrics->numOutputTuple.increase(numValues);
    return true;
}

} // namespace processor
} // namespace kuzu
