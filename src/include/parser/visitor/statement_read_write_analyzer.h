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
    void visitDrop(const Statement& /*statement*/) override { readOnly = false; }
    void visitCreateTable(const Statement& /*statement*/) override { readOnly = false; }
    void visitCreateType(const Statement& /*statement*/) override { readOnly = false; }
    void visitAlter(const Statement& /*statement*/) override { readOnly = false; }
    void visitCopyFrom(const Statement& /*statement*/) override { readOnly = false; }
    void visitStandaloneCall(const Statement& /*statement*/) override { readOnly = true; }
    void visitStandaloneCallFunction(const Statement& /*statement*/) override { readOnly = false; }
    void visitCreateMacro(const Statement& /*statement*/) override { readOnly = false; }

    void visitReadingClause(const ReadingClause* readingClause) override;
    void visitWithClause(const WithClause* withClause) override;
    void visitReturnClause(const ReturnClause* returnClause) override;

    void visitUpdatingClause(const UpdatingClause* /*updatingClause*/) override {
        readOnly = false;
    }

private:
    bool readOnly;
};

} // namespace parser
} // namespace kuzu
