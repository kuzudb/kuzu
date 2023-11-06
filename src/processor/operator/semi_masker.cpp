#include "processor/operator/semi_masker.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void BaseSemiMasker::initGlobalStateInternal(ExecutionContext* /*context*/) {
    for (auto& [table, masks] : info->masksPerTable) {
        for (auto& maskWithIdx : masks) {
            auto maskIdx = maskWithIdx.first->getNumMasks();
            KU_ASSERT(maskIdx < UINT8_MAX);
            maskWithIdx.first->incrementNumMasks();
            maskWithIdx.second = maskIdx;
        }
    }
}

void BaseSemiMasker::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
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
    auto pathRelsFieldIdx = StructType::getFieldIdx(&keyVector->dataType, InternalKeyword::RELS);
    pathRelsVector = StructVector::getFieldVector(keyVector, pathRelsFieldIdx).get();
    auto pathRelsDataVector = ListVector::getDataVector(pathRelsVector);
    auto pathRelsSrcIDFieldIdx =
        StructType::getFieldIdx(&pathRelsDataVector->dataType, InternalKeyword::SRC);
    pathRelsSrcIDDataVector =
        StructVector::getFieldVector(pathRelsDataVector, pathRelsSrcIDFieldIdx).get();
    auto pathRelsDstIDFieldIdx =
        StructType::getFieldIdx(&pathRelsDataVector->dataType, InternalKeyword::DST);
    pathRelsDstIDDataVector =
        StructVector::getFieldVector(pathRelsDataVector, pathRelsDstIDFieldIdx).get();
}

bool PathSingleTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto size = ListVector::getDataVectorSize(pathRelsVector);
    for (auto i = 0u; i < size; ++i) {
        auto srcNodeID = pathRelsSrcIDDataVector->getValue<nodeID_t>(i);
        for (auto& [mask, maskerIdx] : info->getSingleTableMasks()) {
            mask->incrementMaskValue(srcNodeID.offset, maskerIdx);
        }
        auto dstNodeID = pathRelsDstIDDataVector->getValue<nodeID_t>(i);
        for (auto& [mask, maskerIdx] : info->getSingleTableMasks()) {
            mask->incrementMaskValue(dstNodeID.offset, maskerIdx);
        }
    }
    metrics->numOutputTuple.increase(size);
    return true;
}

bool PathMultipleTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto size = ListVector::getDataVectorSize(pathRelsVector);
    for (auto i = 0u; i < size; ++i) {
        auto srcNodeID = pathRelsSrcIDDataVector->getValue<nodeID_t>(i);
        for (auto& [mask, maskerIdx] : info->getTableMasks(srcNodeID.tableID)) {
            mask->incrementMaskValue(srcNodeID.offset, maskerIdx);
        }
        auto dstNodeID = pathRelsDstIDDataVector->getValue<nodeID_t>(i);
        for (auto& [mask, maskerIdx] : info->getTableMasks(dstNodeID.tableID)) {
            mask->incrementMaskValue(dstNodeID.offset, maskerIdx);
        }
    }
    metrics->numOutputTuple.increase(size);
    return true;
}

} // namespace processor
} // namespace kuzu
