#include "processor/operator/var_length_extend/var_length_adj_list_extend.h"

#include "common/types/types.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

AdjListExtendDFSLevelInfo::AdjListExtendDFSLevelInfo(uint8_t level, ExecutionContext& context)
    : DFSLevelInfo{level, context}, parent{0}, childrenIdx{0} {
    // The children ValueVector inside the DFSLevelInfo struct is not tied to a DataChunk.
    // Because we use AdjLists to read data into the children valueVector, and AdjLists requires a
    // DataChunkState to write how many nodes it has read, we create a new DataChunkState and assign
    // it to children.
    children->state = std::make_shared<DataChunkState>();
    listSyncState = std::make_unique<ListSyncState>();
    listHandle = std::make_unique<ListHandle>(*listSyncState);
}

void AdjListExtendDFSLevelInfo::reset(uint64_t parent_) {
    // We do not explicitly reset the largeListHandle because when we call
    // largeListHandle->hasMoreToRead() inside getNextBatchOfNbrNodes() and that returns false,
    // largeListHandle->hasMoreToRead() resets its listSyncState.
    this->parent = parent_;
    this->childrenIdx = 0;
    this->hasBeenOutput = false;
}

void VarLengthAdjListExtend::initLocalStateInternal(
    ResultSet* resultSet, ExecutionContext* context) {
    VarLengthExtend::initLocalStateInternal(resultSet, context);
    for (uint8_t i = 0; i < upperBound; i++) {
        dfsLevelInfos[i] = std::make_shared<AdjListExtendDFSLevelInfo>(i + 1, *context);
    }
}

bool VarLengthAdjListExtend::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        while (!dfsStack.empty()) {
            auto dfsLevelInfo = static_pointer_cast<AdjListExtendDFSLevelInfo>(dfsStack.top());
            if (dfsLevelInfo->level >= lowerBound && dfsLevelInfo->level <= upperBound &&
                !dfsLevelInfo->hasBeenOutput) {
                // It is impossible for the children to have a null value, so we don't need
                // to copy the null mask to the nbrNodeValueVector.
                memcpy(nbrNodeValueVector->getData(), dfsLevelInfo->children->getData(),
                    dfsLevelInfo->children->state->selVector->selectedSize *
                        Types::getDataTypeSize(dfsLevelInfo->children->dataType));
                nbrNodeValueVector->state->selVector->selectedSize =
                    dfsLevelInfo->children->state->selVector->selectedSize;
                dfsLevelInfo->hasBeenOutput = true;
                return true;
            } else if (dfsLevelInfo->childrenIdx <
                           dfsLevelInfo->children->state->selVector->selectedSize &&
                       dfsLevelInfo->level != upperBound) {
                addDFSLevelToStackIfParentExtends(
                    dfsLevelInfo->children->readNodeOffset(
                        dfsLevelInfo->children->state->selVector
                            ->selectedPositions[dfsLevelInfo->childrenIdx]),
                    dfsLevelInfo->level + 1);
                dfsLevelInfo->childrenIdx++;
            } else if (getNextBatchOfNbrNodes(dfsLevelInfo)) {
                dfsLevelInfo->childrenIdx = 0;
                dfsLevelInfo->hasBeenOutput = false;
            } else {
                dfsStack.pop();
            }
        }
        uint64_t curIdx;
        do {
            if (!children[0]->getNextTuple(context)) {
                return false;
            }
            curIdx = boundNodeValueVector->state->selVector->selectedPositions[0];
        } while (boundNodeValueVector->isNull(curIdx) ||
                 !addDFSLevelToStackIfParentExtends(
                     boundNodeValueVector->readNodeOffset(curIdx), 1 /* level */));
    }
}

bool VarLengthAdjListExtend::addDFSLevelToStackIfParentExtends(uint64_t parent, uint8_t level) {
    auto dfsLevelInfo = static_pointer_cast<AdjListExtendDFSLevelInfo>(dfsLevelInfos[level - 1]);
    dfsLevelInfo->reset(parent);
    ((AdjLists*)storage)
        ->initListReadingState(parent, *dfsLevelInfo->listHandle, transaction->getType());
    ((AdjLists*)storage)
        ->readValues(transaction, dfsLevelInfo->children.get(), *dfsLevelInfo->listHandle);
    if (dfsLevelInfo->children->state->selVector->selectedSize != 0) {
        dfsStack.emplace(std::move(dfsLevelInfo));
        return true;
    }
    return false;
}

bool VarLengthAdjListExtend::getNextBatchOfNbrNodes(
    std::shared_ptr<AdjListExtendDFSLevelInfo>& dfsLevel) const {
    if (dfsLevel->listHandle->hasMoreAndSwitchSourceIfNecessary()) {
        ((AdjLists*)storage)
            ->readValues(transaction, dfsLevel->children.get(), *dfsLevel->listHandle);
        return true;
    }
    return false;
}

} // namespace processor
} // namespace kuzu
