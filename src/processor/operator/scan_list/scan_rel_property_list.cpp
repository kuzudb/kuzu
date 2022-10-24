#include "include/scan_rel_property_list.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ScanRelPropertyList::init(ExecutionContext* context) {
    resultSet = ScanList::init(context);
    outValueVector = make_shared<ValueVector>(
        listsWithAdjAndPropertyListsUpdateStore->dataType, context->memoryManager);
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    listHandle = make_shared<ListHandle>(*resultSet->getListSyncState(outDataPos.dataChunkPos));
    return resultSet;
}

bool ScanRelPropertyList::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    outValueVector->resetOverflowBuffer();
    listsWithAdjAndPropertyListsUpdateStore->readValues(outValueVector, *listHandle);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
