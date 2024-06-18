#include "parser/visitor/statement_read_write_analyzer.h"

#include "extension/extension_clause.h"

namespace kuzu {
namespace parser {

bool StatementReadWriteAnalyzer::isReadOnly(const Statement& statement) {
    visit(statement);
    return readOnly;
}

void StatementReadWriteAnalyzer::visitExtensionClause(const Statement& statement) {
    auto& extensionStatement = statement.constCast<extension::ExtensionClause>();
    readOnly = extensionStatement.getExtensionClauseHandler().readWriteAnalyzer(extensionStatement);
}

} // namespace parser
} // namespace kuzu
