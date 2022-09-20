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
    listHandle->reset();
    children[0]->reInitToRerunSubPlan();
}

} // namespace processor
} // namespace graphflow
