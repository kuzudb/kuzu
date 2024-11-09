#include "processor/operator/semi_masker.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

SemiMaskerLocalState* SemiMaskerSharedState::appendLocalState() {
    auto localInfo = std::make_unique<SemiMaskerLocalState>();
    bool isSingle = masksPerTable.size() == 1;
    for (const auto& [tableID, vector] : masksPerTable) {
        auto& mask = vector.front();
        auto newOne = common::RoaringBitmapSemiMaskUtil::createRoaringBitmapSemiMask(
            mask->getTableID(), mask->getMaxOffset());
        if (isSingle) {
            localInfo->singleTableRef = newOne.get();
        }
        localInfo->localMasksPerTable.insert({tableID, std::move(newOne)});
    }
    auto result = localInfo.get();
    std::unique_lock lock{mtx};
    localInfos.push_back(std::move(localInfo));
    return result;
}

void SemiMaskerSharedState::mergeToGlobal() {
    for (const auto& [tableID, globalVector] : masksPerTable) {
        if (globalVector.front()->getMaxOffset() > std::numeric_limits<uint32_t>::max()) {
            std::vector<roaring::Roaring64Map*> masks;
            for (const auto& localInfo : localInfos) {
                const auto& mask = localInfo->localMasksPerTable.at(tableID);
                auto mask64 = static_cast<common::Roaring64BitmapSemiMask*>(mask.get());
                if (!mask64->roaring->isEmpty()) {
                    masks.push_back(mask64->roaring.get());
                }
            }
            auto mergedMask = std::make_shared<roaring::Roaring64Map>(
                roaring::Roaring64Map::fastunion(masks.size(),
                    const_cast<const roaring::Roaring64Map**>(masks.data())));
            for (const auto& item : globalVector) {
                auto mask64 = static_cast<common::Roaring64BitmapSemiMask*>(item);
                mask64->roaring = mergedMask;
            }
        } else {
            std::vector<roaring::Roaring*> masks;
            for (const auto& localInfo : localInfos) {
                const auto& mask = localInfo->localMasksPerTable.at(tableID);
                auto mask32 = static_cast<common::Roaring32BitmapSemiMask*>(mask.get());
                if (!mask32->roaring->isEmpty()) {
                    masks.push_back(mask32->roaring.get());
                }
            }
            auto mergedMask = std::make_shared<roaring::Roaring>(roaring::Roaring::fastunion(
                masks.size(), const_cast<const roaring::Roaring**>(masks.data())));
            for (const auto& item : globalVector) {
                auto mask32 = static_cast<common::Roaring32BitmapSemiMask*>(item);
                mask32->roaring = mergedMask;
            }
        }
    }
}

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
    keyVector = resultSet->getValueVector(keyPos).get();
    localState = sharedState->appendLocalState();
}

void BaseSemiMasker::finalizeInternal(ExecutionContext* /*context*/) {
    sharedState->mergeToGlobal();
}

bool SingleTableSemiMasker::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto& selVector = keyVector->state->getSelVector();
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto pos = selVector[i];
        auto nodeID = keyVector->getValue<nodeID_t>(pos);
        localState->singleTableRef->mask(nodeID.offset);
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
        localState->localMasksPerTable[nodeID.tableID]->mask(nodeID.offset);
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
    auto masker = localState->singleTableRef;
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
                localState->localMasksPerTable.at(srcNodeID.tableID)->mask(srcNodeID.offset);
            }
            if (direction == ExtendDirection::BWD || direction == ExtendDirection::BOTH) {
                auto dstNodeID = pathRelsDstIDDataVector->getValue<nodeID_t>(pos);
                localState->localMasksPerTable.at(dstNodeID.tableID)->mask(dstNodeID.offset);
            }
        }
    }
    uint64_t num = 0;
    for (const auto& [_, masker] : localState->localMasksPerTable) {
        num += masker->getNumMaskedNodes();
    }
    metrics->numOutputTuple.increase(num);
    return true;
}

} // namespace processor
} // namespace kuzu
