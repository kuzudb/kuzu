#include "processor/operator/scan_list/scan_rel_property_list.h"

namespace kuzu {
namespace processor {

void ScanRelPropertyList::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanList::initLocalStateInternal(resultSet, context);
    listHandle = make_shared<ListHandle>(*resultSet->getListSyncState(outDataPos.dataChunkPos));
}

bool ScanRelPropertyList::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    outValueVector->resetOverflowBuffer();
    lists->readValues(outValueVector, *listHandle);
    lists->setDeletedRelsIfNecessary(transaction, listHandle->listSyncState, outValueVector);
    return true;
}

} // namespace processor
} // namespace kuzu
