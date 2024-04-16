#include "processor/operator/scan/scan_rel_table.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    readState = std::make_unique<RelTableScanState>(relInfo.columnIDs, relInfo.direction);
    initVectors(*readState, *resultSet);
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    auto transaction = context->clientContext->getTx();
    while (true) {
        if (readState->hasMoreToRead(transaction)) {
            relInfo.table->scan(transaction, *readState);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        relInfo.table->initializeScanState(transaction, *readState);
    }
}

} // namespace processor
} // namespace kuzu
