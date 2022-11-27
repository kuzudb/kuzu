#include "processor/operator/var_length_extend/var_length_column_extend.h"

#include "common/types/types.h"

namespace kuzu {
namespace processor {

void ColumnExtendDFSLevelInfo::reset() {
    this->hasBeenExtended = false;
    this->hasBeenOutput = false;
}

shared_ptr<ResultSet> VarLengthColumnExtend::init(ExecutionContext* context) {
    VarLengthExtend::init(context);
    for (uint8_t i = 0; i < upperBound; i++) {
        auto dfsLevelInfo = make_shared<ColumnExtendDFSLevelInfo>(i + 1, *context);
        // Since we use boundNodeValueVector as the input valueVector and dfsLevelInfo->children as
        // the output valueVector to the Column::readValues(), and this function requires that the
        // input and output valueVector are in the same dataChunk. As a result, we need to put the
        // dfsLevelInfo->children to the boundNodeValueVector's dataChunk.
        // We can't add the dfsLevelInfo->children to the boundNodeValueVector's dataChunk in the
        // constructor, because the boundNodeValueVector hasn't been initialized in the constructor.
        dfsLevelInfo->children->state = boundNodeValueVector->state;
        dfsLevelInfos[i] = std::move(dfsLevelInfo);
    }
    return resultSet;
}

bool VarLengthColumnExtend::getNextTuplesInternal() {
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
                auto elementSize = Types::getDataTypeSize(dfsLevelInfo->children->dataType);
                memcpy(nbrNodeValueVector->getData() +
                           elementSize * nbrNodeValueVector->state->getPositionOfCurrIdx(),
                    dfsLevelInfo->children->getData() +
                        elementSize * dfsLevelInfo->children->state->getPositionOfCurrIdx(),
                    elementSize);
                dfsLevelInfo->hasBeenOutput = true;
                return true;
            } else if (!dfsLevelInfo->hasBeenExtended && dfsLevelInfo->level != upperBound) {
                addDFSLevelToStackIfParentExtends(dfsLevelInfo->children, dfsLevelInfo->level + 1);
                dfsLevelInfo->hasBeenExtended = true;
            } else {
                dfsStack.pop();
            }
        }
        do {
            if (!children[0]->getNextTuple()) {
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
    ((Column*)storage)->read(transaction, parentValueVector, dfsLevelInfo->children);
    if (!dfsLevelInfo->children->isNull(parentValueVector->state->getPositionOfCurrIdx())) {
        dfsStack.emplace(std::move(dfsLevelInfo));
        return true;
    }
    return false;
}

} // namespace processor
} // namespace kuzu
