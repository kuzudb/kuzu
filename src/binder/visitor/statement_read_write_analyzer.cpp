#include "binder/visitor/statement_read_write_analyzer.h"

namespace kuzu {
namespace binder {

bool StatementReadWriteAnalyzer::isReadOnly(const kuzu::binder::BoundStatement& statement) {
    visit(statement);
    return readOnly;
}

void StatementReadWriteAnalyzer::visitQueryPart(const NormalizedQueryPart& queryPart) {
    readOnly = !queryPart.hasUpdatingClause();
}

} // namespace binder
} // namespace kuzu
