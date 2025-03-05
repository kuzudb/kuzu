#include "processor/operator/scan/primary_key_scan_node_table.h"

#include "binder/expression/expression_util.h"
#include "processor/execution_context.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::string PrimaryKeyScanPrintInfo::toString() const {
    std::string result = "Key: ";
    result += key;
    if (!alias.empty()) {
        result += ",Alias: ";
        result += alias;
    }
    result += ", Expressions: ";
    result += binder::ExpressionUtil::toString(expressions);

    return result;
}

idx_t PrimaryKeyScanSharedState::getTableIdx() {
    std::unique_lock lck{mtx};
    if (cursor < numTables) {
        return cursor++;
    }
    return numTables;
}

void PrimaryKeyScanNodeTable::initLocalStateInternal(ResultSet* resultSet,
    ExecutionContext* context) {
    std::vector<ValueVector*> outVectors;
    for (auto& pos : info.outVectorsPos) {
        outVectors.push_back(resultSet->getValueVector(pos).get());
    }
    scanState = std::make_unique<NodeTableScanState>(
        resultSet->getValueVector(info.nodeIDPos).get(), outVectors, outVectors[0]->state);
    indexEvaluator->init(*resultSet, context->clientContext);
}

bool PrimaryKeyScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    auto transaction = context->clientContext->getTransaction();
    auto tableIdx = sharedState->getTableIdx();
    if (tableIdx >= nodeInfos.size()) {
        return false;
    }
    KU_ASSERT(tableIdx < nodeInfos.size());
    auto& nodeInfo = nodeInfos[tableIdx];

    indexEvaluator->evaluate();
    auto indexVector = indexEvaluator->resultVector.get();
    auto& selVector = indexVector->state->getSelVector();
    KU_ASSERT(selVector.getSelSize() == 1);
    auto pos = selVector.getSelectedPositions()[0];
    if (indexVector->isNull(pos)) {
        return false;
    }

    offset_t nodeOffset = 0;
    const bool lookupSucceed = nodeInfo.table->lookupPK(transaction, indexVector, pos, nodeOffset);
    if (!lookupSucceed) {
        return false;
    }
    auto nodeID = nodeID_t{nodeOffset, nodeInfo.table->getTableID()};
    scanState->setToTable(transaction, nodeInfo.table, nodeInfo.columnIDs,
        copyVector(nodeInfo.columnPredicates));
    scanState->nodeIDVector->setValue<nodeID_t>(pos, nodeID);
    nodeInfo.table->initScanState(transaction, *scanState, nodeID.tableID, nodeOffset);
    metrics->numOutputTuple.incrementByOne();
    return nodeInfo.table->lookup(transaction, *scanState);
}

} // namespace processor
} // namespace kuzu
