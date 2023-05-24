#pragma once

#include "binder/bound_statement_visitor.h"

namespace kuzu {
namespace binder {

class StatementReadWriteAnalyzer : public BoundStatementVisitor {
public:
    StatementReadWriteAnalyzer() : BoundStatementVisitor(), readOnly{false} {}

    bool isReadOnly(const BoundStatement& statement);

private:
    void visitQueryPart(const NormalizedQueryPart& queryPart) final;

private:
    bool readOnly;
};

} // namespace binder
} // namespace kuzu
