#include "processor/operator/semi_masker.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void BaseSemiMasker::initGlobalStateInternal(ExecutionContext* context) {
    for (auto& [table, masks] : info->masksPerTable) {
        for (auto& maskWithIdx : masks) {
            auto maskIdx = maskWithIdx.first->getNumMasks();
            assert(maskIdx < UINT8_MAX);
            maskWithIdx.first->incrementNumMasks();
            maskWithIdx.second = maskIdx;
        }
    }
}

void BaseSemiMasker::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    keyVector = resultSet->getValueVector(info->keyPos).get();
    for (auto& [table, masks] : info->masksPerTable) {
        for (auto& maskWithIdx : masks) {
            maskWithIdx.first->init(transaction);
        }
    }
}

bool SingleTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto selVector = keyVector->state->selVector.get();
    for (auto i = 0u; i < selVector->selectedSize; i++) {
        auto pos = selVector->selectedPositions[i];
        auto nodeID = keyVector->getValue<nodeID_t>(pos);
        for (auto& [mask, maskerIdx] : info->getSingleTableMasks()) {
            mask->incrementMaskValue(nodeID.offset, maskerIdx);
        }
    }
    metrics->numOutputTuple.increase(selVector->selectedSize);
    return true;
}

bool MultiTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto selVector = keyVector->state->selVector.get();
    for (auto i = 0u; i < selVector->selectedSize; i++) {
        auto pos = selVector->selectedPositions[i];
        auto nodeID = keyVector->getValue<nodeID_t>(pos);
        for (auto& [mask, maskerIdx] : info->getTableMasks(nodeID.tableID)) {
            mask->incrementMaskValue(nodeID.offset, maskerIdx);
        }
    }
    metrics->numOutputTuple.increase(selVector->selectedSize);
    return true;
}

void PathSemiMasker::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseSemiMasker::initLocalStateInternal(resultSet, context);
    auto pathNodesFieldIdx =
        common::StructType::getFieldIdx(&keyVector->dataType, InternalKeyword::NODES);
    pathNodesVector = StructVector::getFieldVector(keyVector, pathNodesFieldIdx).get();
    auto pathNodesDataVector = ListVector::getDataVector(pathNodesVector);
    auto pathNodesIDFieldIdx =
        StructType::getFieldIdx(&pathNodesDataVector->dataType, InternalKeyword::ID);
    pathNodesIDDataVector =
        StructVector::getFieldVector(pathNodesDataVector, pathNodesIDFieldIdx).get();
}

bool PathSingleTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto size = ListVector::getDataVectorSize(pathNodesVector);
    for (auto i = 0u; i < size; ++i) {
        auto nodeID = pathNodesIDDataVector->getValue<nodeID_t>(i);
        for (auto& [mask, maskerIdx] : info->getSingleTableMasks()) {
            mask->incrementMaskValue(nodeID.offset, maskerIdx);
        }
    }
    metrics->numOutputTuple.increase(size);
    return true;
}

bool PathMultipleTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto size = ListVector::getDataVectorSize(pathNodesVector);
    for (auto i = 0u; i < size; ++i) {
        auto nodeID = pathNodesIDDataVector->getValue<nodeID_t>(i);
        for (auto& [mask, maskerIdx] : info->getTableMasks(nodeID.tableID)) {
            mask->incrementMaskValue(nodeID.offset, maskerIdx);
        }
    }
    metrics->numOutputTuple.increase(size);
    return true;
}

} // namespace processor
} // namespace kuzu
