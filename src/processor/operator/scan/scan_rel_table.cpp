#include "processor/operator/scan/scan_rel_table.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    scanState = std::make_unique<RelTableReadState>(info->columnIDs, info->direction);
    ScanTable::initVectors(*scanState, *resultSet);
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (scanState->hasMoreToRead(context->clientContext->getTx())) {
            info->table->read(context->clientContext->getTx(), *scanState);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        info->table->initializeReadState(context->clientContext->getTx(), info->direction,
            info->columnIDs, *scanState);
    }
}

} // namespace processor
} // namespace kuzu
