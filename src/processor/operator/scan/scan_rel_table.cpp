#include "processor/operator/scan/scan_rel_table.h"

#include "binder/expression/expression_util.h"
#include "processor/execution_context.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/local_storage/local_storage.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::string ScanRelTablePrintInfo::toString() const {
    std::string result = "Tables: ";
    for (auto& tableName : tableNames) {
        result += tableName;
        if (tableName != tableNames.back()) {
            result += ", ";
        }
    }
    if (!alias.empty()) {
        result += ",Alias: ";
        result += alias;
    }
    result += ",Direction: (";
    result += boundNode->toString();
    result += ")";
    switch (direction) {
    case ExtendDirection::FWD: {
        result += "-[";
        result += rel->detailsToString();
        result += "]->";
    } break;
    case ExtendDirection::BWD: {
        result += "<-[";
        result += rel->detailsToString();
        result += "]-";
    } break;
    case ExtendDirection::BOTH: {
        result += "<-[";
        result += rel->detailsToString();
        result += "]->";
    } break;
    default:
        KU_UNREACHABLE;
    }
    result += "(";
    result += nbrNode->toString();
    result += ")";
    if (!properties.empty()) {
        result += ",Properties: ";
        result += binder::ExpressionUtil::toString(properties);
    }
    return result;
}

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    std::vector<ValueVector*> outVectors;
    for (auto& pos : info.outVectorsPos) {
        outVectors.push_back(resultSet->getValueVector(pos).get());
    }
    scanState = std::make_unique<RelTableScanState>(*context->clientContext->getMemoryManager(),
        resultSet->getValueVector(info.nodeIDPos).get(), outVectors, outVectors[0]->state);
    scanState->setToTable(relInfo.table, relInfo.columnIDs, copyVector(relInfo.columnPredicates),
        relInfo.direction);
    if (const auto localRelTable =
            context->clientContext->getTransaction()->getLocalStorage()->getLocalTable(
                relInfo.table->getTableID(), LocalStorage::NotExistAction::RETURN_NULL)) {
        auto localTableColumnIDs =
            LocalRelTable::rewriteLocalColumnIDs(relInfo.direction, scanState->columnIDs);
        scanState->localTableScanState = std::make_unique<LocalRelTableScanState>(*scanState,
            localTableColumnIDs, localRelTable->ptrCast<LocalRelTable>());
    }
}

void ScanRelTable::initVectors(TableScanState& state, const ResultSet& resultSet) const {
    ScanTable::initVectors(state, resultSet);
    KU_ASSERT(!info.outVectorsPos.empty());
    state.rowIdxVector->state = resultSet.getValueVector(info.outVectorsPos[0])->state;
    state.outState = state.rowIdxVector->state.get();
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    const auto transaction = context->clientContext->getTransaction();
    while (true) {
        while (relInfo.table->scan(transaction, *scanState)) {
            if (scanState->outState->getSelVector().getSelSize() > 0) {
                metrics->numOutputTuple.increase(scanState->outState->getSelVector().getSelSize());
                return true;
            }
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        relInfo.table->initScanState(transaction, *scanState);
    }
}

} // namespace processor
} // namespace kuzu
