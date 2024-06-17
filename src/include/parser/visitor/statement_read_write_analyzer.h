#pragma once

#include "parser/parsed_statement_visitor.h"

namespace kuzu {
namespace parser {

class StatementReadWriteAnalyzer final : public StatementVisitor {
public:
    StatementReadWriteAnalyzer() : StatementVisitor{}, readOnly{true} {}

    bool isReadOnly(const Statement& statement);

private:
    inline void visitCreateSequence(const Statement& /*statement*/) override { readOnly = false; }
    inline void visitDropSequence(const Statement& /*statement*/) override { readOnly = false; }
    inline void visitCreateTable(const Statement& /*statement*/) override { readOnly = false; }
    inline void visitDropTable(const Statement& /*statement*/) override { readOnly = false; }
    inline void visitCreateType(const Statement& /*statement*/) override { readOnly = false; }
    inline void visitAlter(const Statement& /*statement*/) override { readOnly = false; }
    inline void visitCopyFrom(const Statement& /*statement*/) override { readOnly = false; }
    inline void visitStandaloneCall(const Statement& /*statement*/) override { readOnly = true; }
    inline void visitCreateMacro(const Statement& /*statement*/) override { readOnly = false; }

    inline void visitUpdatingClause(const UpdatingClause* /*updatingClause*/) override {
        readOnly = false;
    }

private:
    bool readOnly;
};

} // namespace parser
} // namespace kuzu
