#include "processor/operator/scan_list/scan_rel_table_lists.h"

namespace kuzu {
namespace processor {

void ScanRelTableLists::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseExtendAndScanRelProperties::initLocalStateInternal(resultSet, context);
    syncState = make_unique<ListSyncState>();
    adjListHandle = make_shared<ListHandle>(*syncState);
    for (auto& _ : propertyLists) {
        propertyListHandles.push_back(make_shared<ListHandle>(*syncState));
    }
}

bool ScanRelTableLists::getNextTuplesInternal() {
    if (adjListHandle->listSyncState.hasMoreAndSwitchSourceIfNecessary()) {
        adjList->readValues(outNodeIDVector, *adjListHandle);
    } else {
        do {
            if (!children[0]->getNextTuple()) {
                return false;
            }
            auto currentIdx = inNodeIDVector->state->selVector->selectedPositions[0];
            if (inNodeIDVector->isNull(currentIdx)) {
                outNodeIDVector->state->selVector->selectedSize = 0;
                continue;
            }
            auto currentNodeOffset = inNodeIDVector->readNodeOffset(currentIdx);
            ((AdjLists*)adjList)
                ->initListReadingState(currentNodeOffset, *adjListHandle, transaction->getType());
            adjList->readValues(outNodeIDVector, *adjListHandle);
        } while (outNodeIDVector->state->selVector->selectedSize == 0);
    }
    // TODO(Ziyi/Guodong): this is a hidden bug found in this refactor but also exists in master.
    // Our protocol is that an operator cannot output empty result. This is violated when
    // introducing setDeletedRelsIfNecessary() which might set selectedSize = 0. Let me know if my
    // understanding is correct about this.
    scanPropertyLists();
    metrics->numOutputTuple.increase(outNodeIDVector->state->selVector->selectedSize);
    return true;
}

void ScanRelTableLists::scanPropertyLists() {
    for (auto i = 0u; i < propertyLists.size(); ++i) {
        outPropertyVectors[i]->resetOverflowBuffer();
        propertyLists[i]->readValues(outPropertyVectors[i], *propertyListHandles[i]);
        propertyLists[i]->setDeletedRelsIfNecessary(transaction, *syncState, outPropertyVectors[i]);
    }
}

} // namespace processor
} // namespace kuzu
