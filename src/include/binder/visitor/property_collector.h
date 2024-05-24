#pragma once

#include "binder/bound_statement_visitor.h"

namespace kuzu {
namespace binder {

// Collect all property expressions for a given statement.
class PropertyCollector final : public BoundStatementVisitor {
public:
    expression_vector getProperties();

private:
    void visitMatch(const BoundReadingClause& readingClause) override;
    void visitUnwind(const BoundReadingClause& readingClause) override;
    void visitLoadFrom(const BoundReadingClause& readingClause) override;
    void visitTableFunctionCall(const BoundReadingClause&) override;

    void visitSet(const BoundUpdatingClause& updatingClause) override;
    void visitDelete(const BoundUpdatingClause& updatingClause) override;
    void visitInsert(const BoundUpdatingClause& updatingClause) override;
    void visitMerge(const BoundUpdatingClause& updatingClause) override;

    void visitProjectionBody(const BoundProjectionBody& projectionBody) override;
    void visitProjectionBodyPredicate(const std::shared_ptr<Expression>& predicate) override;

    void collectPropertyExpressions(const std::shared_ptr<Expression>& expression);

private:
    expression_set properties;
};

} // namespace binder
} // namespace kuzu
