#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ReadList::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    inDataChunk = resultSet->dataChunks[inDataPos.dataChunkPos];
    inValueVector = inDataChunk->valueVectors[inDataPos.valueVectorPos];
    outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
    return resultSet;
}

void ReadList::reInitToRerunSubPlan() {
    largeListHandle->reset();
    children[0]->reInitToRerunSubPlan();
}

void ReadList::readValuesFromList() {
    auto currentIdx = inDataChunk->state->getPositionOfCurrIdx();
    if (inValueVector->isNull(currentIdx)) {
        outValueVector->state->selVector->selectedSize = 0;
        return;
    }
    auto nodeOffset = inValueVector->readNodeOffset(inDataChunk->state->getPositionOfCurrIdx());
    lists->readValues(nodeOffset, outValueVector, largeListHandle);
}

} // namespace processor
} // namespace graphflow
