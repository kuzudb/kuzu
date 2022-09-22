#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ReadRelPropertyList::init(ExecutionContext* context) {
    resultSet = ReadList::init(context);
    outValueVector = make_shared<ValueVector>(lists->dataType, context->memoryManager);
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    listSyncState = resultSet->getListSyncState(outDataPos.dataChunkPos);
    return resultSet;
}

bool ReadRelPropertyList::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    outValueVector->resetOverflowBuffer();
    readValuesFromList();
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
