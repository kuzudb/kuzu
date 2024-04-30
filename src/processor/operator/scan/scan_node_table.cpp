#include "processor/operator/scan/scan_node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void ScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    readState = std::make_unique<storage::NodeTableReadState>(info->columnIDs);
    ScanTable::initVectors(*readState, *resultSet);
}

bool ScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    info->table->initializeReadState(context->clientContext->getTx(), info->columnIDs, *readState);
    info->table->read(context->clientContext->getTx(), *readState);
    return true;
}

} // namespace processor
} // namespace kuzu
