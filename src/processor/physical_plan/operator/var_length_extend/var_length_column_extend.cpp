#include "src/processor/include/physical_plan/operator/var_length_extend/var_length_column_extend.h"

#include "src/common/types/include/types.h"

namespace graphflow {
namespace processor {

void ColumnExtendDFSLevelInfo::reset() {
    this->hasBeenExtended = false;
    this->hasBeenOutput = false;
}

VarLengthColumnExtend::VarLengthColumnExtend(const DataPos& boundNodeDataPos,
    const DataPos& nbrNodeDataPos, StorageStructure* storage, uint8_t lowerBound,
    uint8_t upperBound, unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
    : VarLengthExtend(boundNodeDataPos, nbrNodeDataPos, storage, lowerBound, upperBound,
          move(child), context, id) {
    for (uint8_t i = 0; i < upperBound; i++) {
        dfsLevelInfos[i] = make_shared<ColumnExtendDFSLevelInfo>(i + 1, context);
    }
}

shared_ptr<ResultSet> VarLengthColumnExtend::initResultSet() {
    VarLengthExtend::initResultSet();
    // Since we use boundNodeValueVector as the input valueVector and dfsLevelInfo->children as the
    // output valueVector to the Column::readValues(), and this function requires that the input and
    // output valueVector are in the same dataChunk. As a result, we need to put the
    // dfsLevelInfo->children to the boundNodeValueVector's dataChunk.
    // We can't add the dfsLevelInfo->children to the boundNodeValueVector's dataChunk in the
    // constructor, because the boundNodeValueVector hasn't been initialized in the constructor.
    for (auto& dfsLevelInfo : dfsLevelInfos) {
        dfsLevelInfo->children->state = boundNodeValueVector->state;
    }
    return resultSet;
}

bool VarLengthColumnExtend::getNextTuples() {
    metrics->executionTime.start();
    // This general loop structure and how we fetch more data from the child operator after the
    // while(true) loop block is almost the same as that in VarLengthAdjListExtend but there are
    // several differences (e.g., we have one less else if branch here), so we are not refactoring.
    while (true) {
        while (!dfsStack.empty()) {
            auto dfsLevelInfo = static_pointer_cast<ColumnExtendDFSLevelInfo>(dfsStack.top());
            if (dfsLevelInfo->level >= lowerBound && dfsLevelInfo->level <= upperBound &&
                !dfsLevelInfo->hasBeenOutput) {
                // It is impossible for the children to have a null value, so we don't need
                // to copy the null mask to the nbrNodeValueVector.
                memcpy(nbrNodeValueVector->values,
                    dfsLevelInfo->children->values +
                        Types::getDataTypeSize(dfsLevelInfo->children->dataType) *
                            dfsLevelInfo->children->state->getPositionOfCurrIdx(),
                    Types::getDataTypeSize(dfsLevelInfo->children->dataType));
                nbrNodeValueVector->state->selectedSize = 1;
                dfsLevelInfo->hasBeenOutput = true;
                metrics->executionTime.stop();
                return true;
            } else if (!dfsLevelInfo->hasBeenExtended && dfsLevelInfo->level != upperBound) {
                addDFSLevelToStackIfParentExtends(dfsLevelInfo->children, dfsLevelInfo->level + 1);
                dfsLevelInfo->hasBeenExtended = true;
            } else {
                dfsStack.pop();
            }
        }
        do {
            if (!children[0]->getNextTuples()) {
                metrics->executionTime.stop();
                return false;
            }
        } while (
            boundNodeValueVector->isNull(boundNodeValueVector->state->getPositionOfCurrIdx()) ||
            !addDFSLevelToStackIfParentExtends(boundNodeValueVector, 1 /* level */));
    }
}

bool VarLengthColumnExtend::addDFSLevelToStackIfParentExtends(
    shared_ptr<ValueVector>& parentValueVector, uint8_t level) {
    auto dfsLevelInfo = static_pointer_cast<ColumnExtendDFSLevelInfo>(dfsLevelInfos[level - 1]);
    dfsLevelInfo->reset();
    ((Column*)storage)->readValues(parentValueVector, dfsLevelInfo->children);
    if (!dfsLevelInfo->children->isNull(parentValueVector->state->getPositionOfCurrIdx())) {
        dfsStack.emplace(move(dfsLevelInfo));
        return true;
    }
    return false;
}

} // namespace processor
} // namespace graphflow
