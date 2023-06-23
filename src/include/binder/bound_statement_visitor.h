#pragma once

#include "binder/query/bound_regular_query.h"
#include "bound_statement.h"

namespace kuzu {
namespace binder {

class BoundStatementVisitor {
public:
    BoundStatementVisitor() = default;
    virtual ~BoundStatementVisitor() = default;

    void visit(const BoundStatement& statement);

    virtual void visitRegularQuery(const BoundRegularQuery& regularQuery);
    virtual void visitSingleQuery(const NormalizedSingleQuery& singleQuery);
    virtual void visitQueryPart(const NormalizedQueryPart& queryPart);

protected:
    virtual void visitCreateNodeTable(const BoundStatement& statement) {}
    virtual void visitCreateRelTable(const BoundStatement& statement) {}
    virtual void visitDropTable(const BoundStatement& statement) {}
    virtual void visitRenameTable(const BoundStatement& statement) {}
    virtual void visitAddProperty(const BoundStatement& statement) {}
    virtual void visitDropProperty(const BoundStatement& statement) {}
    virtual void visitRenameProperty(const BoundStatement& statement) {}
    virtual void visitCopy(const BoundStatement& statement) {}
    virtual void visitCall(const BoundStatement& statement) {}

    void visitReadingClause(const BoundReadingClause& readingClause);
    virtual void visitMatch(const BoundReadingClause& readingClause) {}
    virtual void visitUnwind(const BoundReadingClause& readingClause) {}
    void visitUpdatingClause(const BoundUpdatingClause& updatingClause);
    virtual void visitSet(const BoundUpdatingClause& updatingClause) {}
    virtual void visitDelete(const BoundUpdatingClause& updatingClause) {}
    virtual void visitCreate(const BoundUpdatingClause& updatingClause) {}

    virtual void visitProjectionBody(const BoundProjectionBody& projectionBody) {}
    virtual void visitProjectionBodyPredicate(Expression& predicate) {}
};

} // namespace binder
} // namespace kuzu
