#include "processor/operator/var_length_extend/var_length_column_extend.h"

#include "common/types/types.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void ColumnExtendDFSLevelInfo::reset() {
    this->hasBeenExtended = false;
    this->hasBeenOutput = false;
}

void VarLengthColumnExtend::initLocalStateInternal(
    ResultSet* resultSet, ExecutionContext* context) {
    VarLengthExtend::initLocalStateInternal(resultSet, context);
    for (uint8_t i = 0; i < upperBound; i++) {
        auto dfsLevelInfo = std::make_shared<ColumnExtendDFSLevelInfo>(i + 1, *context);
        // Since we use boundNodeValueVector as the input valueVector and dfsLevelInfo->children as
        // the output valueVector to the Column::readValues(), and this function requires that the
        // input and output valueVector are in the same dataChunk. As a result, we need to put the
        // dfsLevelInfo->children to the boundNodeValueVector's dataChunk.
        // We can't add the dfsLevelInfo->children to the boundNodeValueVector's dataChunk in the
        // constructor, because the boundNodeValueVector hasn't been initialized in the constructor.
        dfsLevelInfo->children->state = boundNodeValueVector->state;
        dfsLevelInfos[i] = std::move(dfsLevelInfo);
    }
}

bool VarLengthColumnExtend::getNextTuplesInternal(ExecutionContext* context) {
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
                           elementSize * nbrNodeValueVector->state->selVector->selectedPositions[0],
                    dfsLevelInfo->children->getData() +
                        elementSize *
                            dfsLevelInfo->children->state->selVector->selectedPositions[0],
                    elementSize);
                dfsLevelInfo->hasBeenOutput = true;
                return true;
            } else if (!dfsLevelInfo->hasBeenExtended && dfsLevelInfo->level != upperBound) {
                addDFSLevelToStackIfParentExtends(
                    dfsLevelInfo->children.get(), dfsLevelInfo->level + 1);
                dfsLevelInfo->hasBeenExtended = true;
            } else {
                dfsStack.pop();
            }
        }
        do {
            if (!children[0]->getNextTuple(context)) {
                return false;
            }
        } while (boundNodeValueVector->isNull(
                     boundNodeValueVector->state->selVector->selectedPositions[0]) ||
                 !addDFSLevelToStackIfParentExtends(boundNodeValueVector, 1 /* level */));
    }
}

bool VarLengthColumnExtend::addDFSLevelToStackIfParentExtends(
    common::ValueVector* parentValueVector, uint8_t level) {
    auto dfsLevelInfo = static_pointer_cast<ColumnExtendDFSLevelInfo>(dfsLevelInfos[level - 1]);
    dfsLevelInfo->reset();
    ((Column*)storage)->read(transaction, parentValueVector, dfsLevelInfo->children.get());
    if (!dfsLevelInfo->children->isNull(
            parentValueVector->state->selVector->selectedPositions[0])) {
        dfsStack.emplace(std::move(dfsLevelInfo));
        return true;
    }
    return false;
}

} // namespace processor
} // namespace kuzu
