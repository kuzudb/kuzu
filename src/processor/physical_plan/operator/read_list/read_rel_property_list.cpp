#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"

#include "src/common/include/date.h"

namespace graphflow {
namespace processor {

void ReadRelPropertyList::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    ReadList::initResultSet(resultSet);
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->getDataType());
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    largeListHandle->setListSyncState(resultSet->getListSyncState(outDataPos.dataChunkPos));
}

void ReadRelPropertyList::getNextTuples() {
    metrics->executionTime.start();
    prevOperator->getNextTuples();
    if (inDataChunk->state->selectedSize > 0) {
        readValuesFromList();
    }
    outValueVector->fillNullMask();
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow
