#include "processor/operator/scan/scan_rel_table.h"

#include "binder/expression/expression_util.h"

#include "storage/local_storage/local_rel_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static std::string relToString(const binder::RelExpression& rel) {
    auto result = rel.toString();
    switch (rel.getRelType()) {
    case QueryRelType::SHORTEST: {
        result += "SHORTEST";
    } break;
    case QueryRelType::ALL_SHORTEST: {
        result += "ALL SHORTEST";
    } break;
    default:
        break;
    }
    if (QueryRelTypeUtils::isRecursive(rel.getRelType())) {
        result += std::to_string(rel.getLowerBound());
        result += "..";
        result += std::to_string(rel.getUpperBound());
    }
    return result;
}

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
        result += relToString(*rel);
        result += "]->";
    } break;
    case ExtendDirection::BWD: {
        result += "<-[";
        result += relToString(*rel);
        result += "]-";
    } break;
    case ExtendDirection::BOTH: {
        result += "<-[";
        result += relToString(*rel);
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

void ScanRelTableInfo::initScanState() {
    std::vector<Column*> columns;
    columns.reserve(columnIDs.size());
    for (const auto columnID : columnIDs) {
        if (columnID == INVALID_COLUMN_ID) {
            columns.push_back(nullptr);
        } else {
            columns.push_back(table->getColumn(columnID, direction));
        }
    }
    scanState = std::make_unique<RelTableScanState>(columnIDs, columns,
        table->getCSROffsetColumn(direction), table->getCSRLengthColumn(direction), direction,
        copyVector(columnPredicates));
}

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    relInfo.initScanState();
    initVectors(*relInfo.scanState, *resultSet);
    if (const auto localRelTable =
            context->clientContext->getTx()->getLocalStorage()->getLocalTable(
                relInfo.table->getTableID(), LocalStorage::NotExistAction::RETURN_NULL)) {
        auto localTableColumnIDs =
            LocalRelTable::rewriteLocalColumnIDs(relInfo.direction, relInfo.scanState->columnIDs);
        relInfo.scanState->localTableScanState = std::make_unique<LocalRelTableScanState>(
            *relInfo.scanState, localTableColumnIDs, localRelTable->ptrCast<LocalRelTable>());
    }
}

void ScanRelTable::initVectors(TableScanState& state, const ResultSet& resultSet) const {
    ScanTable::initVectors(state, resultSet);
    state.cast<RelTableScanState>().boundNodeIDVector =
        resultSet.getValueVector(relInfo.boundNodeIDPos).get();
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
                if (scanState.IDVector->state->getSelVector().getSelSize() > 0) {
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
