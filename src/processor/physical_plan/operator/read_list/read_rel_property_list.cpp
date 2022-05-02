#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ReadRelPropertyList::initResultSet() {
    ReadList::initResultSet();
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->dataType);
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    largeListHandle->setListSyncState(resultSet->getListSyncState(outDataPos.dataChunkPos));
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
