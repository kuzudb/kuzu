#include "processor/operator/semi_masker.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::string SemiMaskerPrintInfo::toString() const {
    std::string result = "Operators: ";
    for (const auto& op : operatorNames) {
        result += op;
        if (&op != &operatorNames.back()) {
            result += ", ";
        }
    }
    return result;
}

void BaseSemiMasker::initLocalStateInternal(ResultSet* resultSet, ExecutionContext*) {
    keyVector = resultSet->getValueVector(info->keyPos).get();
    localInfo = info->appendLocalInfo();
}

void BaseSemiMasker::finalizeInternal(ExecutionContext* /*context*/) {
    info->mergeToGlobalInfo();
}

bool SingleTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto& selVector = keyVector->state->getSelVector();
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto pos = selVector[i];
        auto nodeID = keyVector->getValue<nodeID_t>(pos);
        localInfo->singleTableRef->mask(nodeID.offset);
    }
    metrics->numOutputTuple.increase(selVector.getSelSize());
    return true;
}

bool MultiTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto& selVector = keyVector->state->getSelVector();
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto pos = selVector[i];
        auto nodeID = keyVector->getValue<nodeID_t>(pos);
        localInfo->localMasksPerTable[nodeID.tableID]->mask(nodeID.offset);
    }
    metrics->numOutputTuple.increase(selVector.getSelSize());
    return true;
}

void PathSemiMasker::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseSemiMasker::initLocalStateInternal(resultSet, context);
    auto pathRelsFieldIdx = StructType::getFieldIdx(keyVector->dataType, InternalKeyword::RELS);
    pathRelsVector = StructVector::getFieldVector(keyVector, pathRelsFieldIdx).get();
    auto pathRelsDataVector = ListVector::getDataVector(pathRelsVector);
    auto pathRelsSrcIDFieldIdx =
        StructType::getFieldIdx(pathRelsDataVector->dataType, InternalKeyword::SRC);
    pathRelsSrcIDDataVector =
        StructVector::getFieldVector(pathRelsDataVector, pathRelsSrcIDFieldIdx).get();
    auto pathRelsDstIDFieldIdx =
        StructType::getFieldIdx(pathRelsDataVector->dataType, InternalKeyword::DST);
    pathRelsDstIDDataVector =
        StructVector::getFieldVector(pathRelsDataVector, pathRelsDstIDFieldIdx).get();
}

bool PathSingleTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto& selVector = keyVector->state->getSelVector();
    // for both direction, we should deal with direction based on the actual direction of the edge
    auto masker = localInfo->singleTableRef;
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto [offset, size] = pathRelsVector->getValue<list_entry_t>(selVector[i]);
        for (auto j = 0u; j < size; ++j) {
            auto pos = offset + j;
            if (direction == ExtendDirection::FWD || direction == ExtendDirection::BOTH) {
                auto srcNodeID = pathRelsSrcIDDataVector->getValue<nodeID_t>(pos);
                masker->mask(srcNodeID.offset);
            }
            if (direction == ExtendDirection::BWD || direction == ExtendDirection::BOTH) {
                auto dstNodeID = pathRelsDstIDDataVector->getValue<nodeID_t>(pos);
                masker->mask(dstNodeID.offset);
            }
        }
    }
    metrics->numOutputTuple.increase(masker->getNumMaskedNodes());
    return true;
}

bool PathMultipleTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto& selVector = pathRelsVector->state->getSelVector();
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto [offset, size] = pathRelsVector->getValue<list_entry_t>(selVector[i]);
        for (auto j = 0u; j < size; ++j) {
            auto pos = offset + j;
            if (direction == ExtendDirection::FWD || direction == ExtendDirection::BOTH) {
                auto srcNodeID = pathRelsSrcIDDataVector->getValue<nodeID_t>(pos);
                localInfo->localMasksPerTable.at(srcNodeID.tableID)->mask(srcNodeID.offset);
            }
            if (direction == ExtendDirection::BWD || direction == ExtendDirection::BOTH) {
                auto dstNodeID = pathRelsDstIDDataVector->getValue<nodeID_t>(pos);
                localInfo->localMasksPerTable.at(dstNodeID.tableID)->mask(dstNodeID.offset);
            }
        }
    }
    uint64_t num = 0;
    for (const auto& [_, masker] : localInfo->localMasksPerTable) {
        num += masker->getNumMaskedNodes();
    }
    metrics->numOutputTuple.increase(num);
    return true;
}

} // namespace processor
} // namespace kuzu
