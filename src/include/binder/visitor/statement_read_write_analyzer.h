#pragma once

#include "binder/bound_statement_visitor.h"

namespace kuzu {
namespace binder {

class StatementReadWriteAnalyzer final : public BoundStatementVisitor {
public:
    StatementReadWriteAnalyzer() : BoundStatementVisitor(), readOnly{true} {}

    bool isReadOnly(const BoundStatement& statement);

private:
    void visitCommentOn(const BoundStatement& /*statement*/) { readOnly = false; }
    void visitCopyFrom(const BoundStatement& /*statement*/) { readOnly = false; }
    void visitCreateMacro(const BoundStatement& /*statement*/) { readOnly = false; }
    void visitCreateTable(const BoundStatement& /*statement*/) { readOnly = false; }
    void visitDropTable(const BoundStatement& /*statement*/) { readOnly = false; }
    void visitAlter(const BoundStatement& /*statement*/) { readOnly = false; }
    void visitStandaloneCall(const BoundStatement& /*statement*/) { readOnly = false; }
    void visitQueryPart(const NormalizedQueryPart& queryPart);

private:
    bool readOnly;
};

} // namespace binder
} // namespace kuzu
