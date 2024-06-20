#pragma once

#include "parser/parsed_statement_visitor.h"

namespace kuzu {
namespace parser {

class StatementReadWriteAnalyzer final : public StatementVisitor {
public:
    StatementReadWriteAnalyzer() : StatementVisitor{}, readOnly{true} {}

    bool isReadOnly(const Statement& statement);

private:
    void visitCreateSequence(const Statement& /*statement*/) override { readOnly = false; }
    void visitDropSequence(const Statement& /*statement*/) override { readOnly = false; }
    void visitCreateTable(const Statement& /*statement*/) override { readOnly = false; }
    void visitDropTable(const Statement& /*statement*/) override { readOnly = false; }
    void visitCreateType(const Statement& /*statement*/) override { readOnly = false; }
    void visitAlter(const Statement& /*statement*/) override { readOnly = false; }
    void visitCopyFrom(const Statement& /*statement*/) override { readOnly = false; }
    void visitStandaloneCall(const Statement& /*statement*/) override { readOnly = true; }
    void visitCreateMacro(const Statement& /*statement*/) override { readOnly = false; }
    void visitUpdatingClause(const UpdatingClause* /*updatingClause*/) override {
        readOnly = false;
    }
    void visitExtensionClause(const Statement& statement) override;

private:
    bool readOnly;
};

} // namespace parser
} // namespace kuzu
