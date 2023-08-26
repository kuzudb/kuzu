#pragma once

#include "binder/bound_statement_visitor.h"

namespace kuzu {
namespace binder {

class PropertyCollector : public BoundStatementVisitor {
public:
    PropertyCollector() : BoundStatementVisitor() {}

    expression_vector getProperties();

private:
    void addRDFPredicateIRIOffsetProperty(
        kuzu::binder::PropertyExpression* rdfPredicateIRIProperty, expression_vector& result);
    void visitMatch(const BoundReadingClause& readingClause) final;
    void visitUnwind(const BoundReadingClause& readingClause) final;

    void visitSet(const BoundUpdatingClause& updatingClause) final;
    void visitDelete(const BoundUpdatingClause& updatingClause) final;
    void visitCreate(const BoundUpdatingClause& updatingClause) final;
    void visitMerge(const BoundUpdatingClause& updatingClause) final;

    void visitProjectionBody(const BoundProjectionBody& projectionBody) final;
    void visitProjectionBodyPredicate(const std::shared_ptr<Expression>& predicate) final;

    void collectPropertyExpressions(const std::shared_ptr<Expression>& expression);

private:
    expression_set properties;
    std::vector<std::shared_ptr<RelExpression>> relExpressions;
};

} // namespace binder
} // namespace kuzu
