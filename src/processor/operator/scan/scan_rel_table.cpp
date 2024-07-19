#include "processor/operator/scan/scan_rel_table.h"

#include "binder/expression/expression_util.h"

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
    localScanState =
        std::make_unique<RelTableScanState>(columnIDs, direction, copyVector(columnPredicates));
}

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    relInfo.initScanState();
    initVectors(*relInfo.localScanState, *resultSet);
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    auto transaction = context->clientContext->getTx();
    auto& scanState = *relInfo.localScanState;
    while (true) {
        auto skipScan =
            transaction->isReadOnly() && scanState.zoneMapResult == ZoneMapCheckResult::SKIP_SCAN;
        if (!skipScan && relInfo.localScanState->hasMoreToRead(transaction)) {
            relInfo.table->scan(transaction, scanState);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        relInfo.table->initializeScanState(transaction, scanState);
    }
}

} // namespace processor
} // namespace kuzu
