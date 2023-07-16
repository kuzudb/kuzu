#pragma once

#include "binder/bound_statement_visitor.h"

namespace kuzu {
namespace binder {

class PropertyCollector : public BoundStatementVisitor {
public:
    PropertyCollector() : BoundStatementVisitor() {}

    expression_vector getProperties();

private:
    void visitMatch(const BoundReadingClause& readingClause) final;
    void visitUnwind(const BoundReadingClause& readingClause) final;

    void visitSet(const BoundUpdatingClause& updatingClause) final;
    void visitDelete(const BoundUpdatingClause& updatingClause) final;
    void visitCreate(const BoundUpdatingClause& updatingClause) final;

    void visitProjectionBody(const BoundProjectionBody& projectionBody) final;
    void visitProjectionBodyPredicate(const std::shared_ptr<Expression>& predicate) final;

    void collectPropertyExpressions(const std::shared_ptr<Expression>& expression);

private:
    expression_set properties;
};

} // namespace binder
} // namespace kuzu
