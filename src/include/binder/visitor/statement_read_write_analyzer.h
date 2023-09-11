#pragma once

#include "binder/bound_statement_visitor.h"

namespace kuzu {
namespace binder {

class StatementReadWriteAnalyzer : public BoundStatementVisitor {
public:
    StatementReadWriteAnalyzer() : BoundStatementVisitor(), readOnly{true} {}

    bool isReadOnly(const BoundStatement& statement);

private:
    void visitCreateTable(const BoundStatement& statement) override { readOnly = false; }
    void visitDropTable(const BoundStatement& statement) override { readOnly = false; }
    void visitRenameTable(const BoundStatement& statement) override { readOnly = false; }
    void visitAddProperty(const BoundStatement& statement) override { readOnly = false; }
    void visitDropProperty(const BoundStatement& statement) override { readOnly = false; }
    void visitRenameProperty(const BoundStatement& statement) override { readOnly = false; }
    void visitCopy(const BoundStatement& statement) override { readOnly = false; }
    void visitCreateMacro(const BoundStatement& statement) override { readOnly = false; }
    void visitQueryPart(const NormalizedQueryPart& queryPart) final;

private:
    bool readOnly;
};

} // namespace binder
} // namespace kuzu
