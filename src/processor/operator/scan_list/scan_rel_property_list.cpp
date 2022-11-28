#include "processor/operator/scan_list/scan_rel_property_list.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> ScanRelPropertyList::init(ExecutionContext* context) {
    resultSet = ScanList::init(context);
    outValueVector = make_shared<ValueVector>(lists->dataType, context->memoryManager);
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    listHandle = make_shared<ListHandle>(*resultSet->getListSyncState(outDataPos.dataChunkPos));
    return resultSet;
}

bool ScanRelPropertyList::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    outValueVector->resetOverflowBuffer();
    lists->readValues(outValueVector, *listHandle);
    return true;
}

} // namespace processor
} // namespace kuzu
