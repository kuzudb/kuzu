#include "processor/operator/scan/scan_rel_table.h"

#include "binder/expression/expression_util.h"
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

void ScanRelTableInfo::initScanState(MemoryManager& memoryManager) {
    std::vector<Column*> columns;
    columns.reserve(columnIDs.size());
    for (const auto columnID : columnIDs) {
        if (columnID == INVALID_COLUMN_ID) {
            columns.push_back(nullptr);
        } else {
            KU_ASSERT(columnID < table->getNumColumns());
            columns.push_back(table->getColumn(columnID, direction));
        }
    }
    scanState = std::make_unique<RelTableScanState>(memoryManager, columnIDs, columns,
        table->getCSROffsetColumn(direction), table->getCSRLengthColumn(direction), direction,
        copyVector(columnPredicates));
}

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    relInfo.initScanState(*context->clientContext->getMemoryManager());
    initVectors(*relInfo.scanState, *resultSet);
    if (const auto localRelTable =
            context->clientContext->getTx()->getLocalStorage()->getLocalTable(
                relInfo.table->getTableID(), LocalStorage::NotExistAction::RETURN_NULL)) {
        auto localTableColumnIDs =
            LocalRelTable::rewriteLocalColumnIDs(relInfo.direction, relInfo.scanState->columnIDs);
        relInfo.scanState->localTableScanState =
            std::make_unique<LocalRelTableScanState>(*context->clientContext->getMemoryManager(),
                *relInfo.scanState, localTableColumnIDs, localRelTable->ptrCast<LocalRelTable>());
    }
}

void ScanRelTable::initVectors(TableScanState& state, const ResultSet& resultSet) const {
    ScanTable::initVectors(state, resultSet);
    KU_ASSERT(!info.outVectorsPos.empty());
    state.rowIdxVector->state = resultSet.getValueVector(info.outVectorsPos[0])->state;
    state.outState = state.rowIdxVector->state.get();
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    const auto transaction = context->clientContext->getTx();
    auto& scanState = *relInfo.scanState;
    while (true) {
        const auto skipScan =
            transaction->isReadOnly() && scanState.zoneMapResult == ZoneMapCheckResult::SKIP_SCAN;
        if (!skipScan) {
            while (scanState.source != TableScanSource::NONE &&
                   relInfo.table->scan(transaction, scanState)) {
                if (scanState.outState->getSelVector().getSelSize() > 0) {
                    return true;
                }
            }
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        relInfo.table->initializeScanState(transaction, scanState);
    }
}

} // namespace processor
} // namespace kuzu
