#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"

#include "src/common/include/date.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ReadRelPropertyList::initResultSet() {
    ReadList::initResultSet();
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->getDataType());
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    largeListHandle->setListSyncState(resultSet->getListSyncState(outDataPos.dataChunkPos));
    return resultSet;
}

bool ReadRelPropertyList::getNextTuples() {
    metrics->executionTime.start();
    if (!prevOperator->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    readValuesFromList();
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
