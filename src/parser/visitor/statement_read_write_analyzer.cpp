#include "parser/visitor/statement_read_write_analyzer.h"

namespace kuzu {
namespace parser {

bool StatementReadWriteAnalyzer::isReadOnly(const Statement& statement) {
    visit(statement);
    return readOnly;
}

} // namespace parser
} // namespace kuzu
