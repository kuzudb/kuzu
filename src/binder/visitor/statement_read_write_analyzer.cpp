#include "binder/visitor/statement_read_write_analyzer.h"

namespace kuzu {
namespace binder {

bool StatementReadWriteAnalyzer::isReadOnly(const kuzu::binder::BoundStatement& statement) {
    visit(statement);
    return readOnly;
}

void StatementReadWriteAnalyzer::visitQueryPart(const NormalizedQueryPart& queryPart) {
    if (queryPart.hasUpdatingClause()) {
        readOnly = false;
    }
}

} // namespace binder
} // namespace kuzu
