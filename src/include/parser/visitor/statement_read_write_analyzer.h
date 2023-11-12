#pragma once

#include "parser/parsed_statement_visitor.h"

namespace kuzu {
namespace parser {

class StatementReadWriteAnalyzer final : public StatementVisitor {
public:
    StatementReadWriteAnalyzer() : StatementVisitor{}, readOnly{true} {}

    bool isReadOnly(const Statement& statement);

private:
    inline void visitCreateTable(const Statement& /*statement*/) { readOnly = false; }
    inline void visitDropTable(const Statement& /*statement*/) { readOnly = false; }
    inline void visitAlter(const Statement& /*statement*/) { readOnly = false; }
    inline void visitCopyFrom(const Statement& /*statement*/) { readOnly = false; }
    inline void visitStandaloneCall(const Statement& /*statement*/) { readOnly = false; }
    inline void visitCreateMacro(const Statement& /*statement*/) { readOnly = false; }
    inline void visitCommentOn(const Statement& /*statement*/) { readOnly = false; }

    inline void visitUpdatingClause(const UpdatingClause* /*updatingClause*/) { readOnly = false; }

private:
    bool readOnly;
};

} // namespace parser
} // namespace kuzu
