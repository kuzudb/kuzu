#include "main/query_summary.h"

#include "common/statement_type.h"

namespace kuzu {
namespace main {

double QuerySummary::getCompilingTime() const {
    return preparedSummary.compilingTime;
}

double QuerySummary::getExecutionTime() const {
    return executionTime;
}

void QuerySummary::setPreparedSummary(PreparedSummary preparedSummary_) {
    preparedSummary = preparedSummary_;
}

bool QuerySummary::isExplain() const {
    return preparedSummary.statementType == common::StatementType::EXPLAIN;
}

} // namespace main
} // namespace kuzu
