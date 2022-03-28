#include "src/processor/include/physical_plan/operator/var_length_adj_list_extend.h"

#include "src/common/types/include/types.h"

namespace graphflow {
namespace processor {

DFSLevelInfo::DFSLevelInfo(uint8_t level, ExecutionContext& context)
    : level{level}, parent{0ul}, childrenIdx{0ul}, hasBeenOutput{false} {
    // The children ValueVector inside the DFSLevelInfo struct is not tied to a DataChunk.
    // Because we use AdjLists to read data into the children valueVector, and AdjLists requires a
    // DataChunkState to write how many nodes it has read, we create a new DataChunkState and assign
    // it to children.
    children = make_shared<ValueVector>(context.memoryManager, NODE);
    children->state = make_shared<DataChunkState>();
    largeListHandle = make_unique<LargeListHandle>(true /* isAdjList */);
    largeListHandle->setListSyncState(make_shared<ListSyncState>());
}

void DFSLevelInfo::reset(uint64_t parent) {
    // We do not explicitly reset the largeListHandle because when we call
    // largeListHandle->hasMoreToRead() inside getNextBatchOfNbrNodes() and that returns false,
    // largeListHandle->hasMoreToRead() resets its listSyncState.
    this->parent = parent;
    this->childrenIdx = 0;
    this->hasBeenOutput = false;
}

VarLengthAdjListExtend::VarLengthAdjListExtend(const DataPos& boundNodeDataPos,
    const DataPos& nbrNodeDataPos, AdjLists* adjLists, uint8_t lowerBound, uint8_t upperBound,
    unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(child), context, id}, boundNodeDataPos{boundNodeDataPos},
      nbrNodeDataPos{nbrNodeDataPos}, adjLists{adjLists}, lowerBound{lowerBound}, upperBound{
                                                                                      upperBound} {
    dfsLevelInfos.resize(upperBound);
    for (uint8_t i = 0; i < upperBound; i++) {
        dfsLevelInfos[i] = make_shared<DFSLevelInfo>(i + 1, context);
    }
}

shared_ptr<ResultSet> VarLengthAdjListExtend::initResultSet() {
    resultSet = children[0]->initResultSet();
    boundNodeValueVector = resultSet->dataChunks[boundNodeDataPos.dataChunkPos]
                               ->valueVectors[boundNodeDataPos.valueVectorPos];
    nbrNodeValueVector = make_shared<ValueVector>(context.memoryManager, NODE);
    resultSet->dataChunks[nbrNodeDataPos.dataChunkPos]->insert(
        nbrNodeDataPos.valueVectorPos, nbrNodeValueVector);
    return resultSet;
}

bool VarLengthAdjListExtend::getNextTuples() {
    metrics->executionTime.start();
    while (true) {
        while (!dfsStack.empty()) {
            auto dfsLevelInfo = dfsStack.top();
            if (dfsLevelInfo->level >= lowerBound && dfsLevelInfo->level <= upperBound &&
                !dfsLevelInfo->hasBeenOutput) {
                dfsLevelInfo->hasBeenOutput = true;
                // It is impossible for the children to have a null value, so we don't need
                // to copy the null mask to the nbrNodeValueVector.
                memcpy(nbrNodeValueVector->values.get(), dfsLevelInfo->children->values.get(),
                    dfsLevelInfo->children->state->selectedSize *
                        Types::getDataTypeSize(dfsLevelInfo->children->dataType));
                nbrNodeValueVector->state->selectedSize =
                    dfsLevelInfo->children->state->selectedSize;
                metrics->executionTime.stop();
                return true;
            } else if (dfsLevelInfo->childrenIdx < dfsLevelInfo->children->state->selectedSize &&
                       dfsLevelInfo->level != upperBound) {
                addDFSLevelToStackIfParentExtends(
                    dfsLevelInfo->children->readNodeOffset(
                        dfsLevelInfo->children->state
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
            if (!children[0]->getNextTuples()) {
                metrics->executionTime.stop();
                return false;
            }
            curIdx = boundNodeValueVector->state->getPositionOfCurrIdx();
        } while (boundNodeValueVector->isNull(curIdx) ||
                 !addDFSLevelToStackIfParentExtends(
                     boundNodeValueVector->readNodeOffset(curIdx), 1 /* level */));
    }
}

bool VarLengthAdjListExtend::addDFSLevelToStackIfParentExtends(uint64_t parent, uint8_t level) {
    auto dfsLevelInfo = dfsLevelInfos[level - 1];
    dfsLevelInfo->reset(parent);
    adjLists->readValues(parent, dfsLevelInfo->children, dfsLevelInfo->largeListHandle);
    if (dfsLevelInfo->children->state->selectedSize != 0) {
        dfsStack.emplace(move(dfsLevelInfo));
        return true;
    }
    return false;
}

bool VarLengthAdjListExtend::getNextBatchOfNbrNodes(shared_ptr<DFSLevelInfo>& dfsLevel) const {
    if (dfsLevel->largeListHandle->hasMoreToRead()) {
        adjLists->readValues(dfsLevel->parent, dfsLevel->children, dfsLevel->largeListHandle);
        return true;
    }
    return false;
}

} // namespace processor
} // namespace graphflow
