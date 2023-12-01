#pragma once

#include "binder/query/normalized_single_query.h"
#include "bound_statement.h"

namespace kuzu {
namespace binder {

class BoundStatementVisitor {
public:
    BoundStatementVisitor() = default;
    virtual ~BoundStatementVisitor() = default;

    void visit(const BoundStatement& statement);

    virtual void visitSingleQuery(const NormalizedSingleQuery& singleQuery);

protected:
    virtual void visitCreateTable(const BoundStatement& statement) {}
    virtual void visitDropTable(const BoundStatement& statement) {}
    virtual void visitAlter(const BoundStatement& statement) {}
    virtual void visitCopyFrom(const BoundStatement& statement) {}
    virtual void visitCopyTo(const BoundStatement& statement) {}
    virtual void visitStandaloneCall(const BoundStatement& statement) {}
    virtual void visitCommentOn(const BoundStatement& statement) {}
    virtual void visitExplain(const BoundStatement& statement);
    virtual void visitCreateMacro(const BoundStatement& statement) {}
    virtual void visitTransaction(const BoundStatement& statement) {}

    virtual void visitRegularQuery(const BoundStatement& statement);
    virtual void visitQueryPart(const NormalizedQueryPart& queryPart);
    void visitReadingClause(const BoundReadingClause& readingClause);
    virtual void visitMatch(const BoundReadingClause& readingClause) {}
    virtual void visitUnwind(const BoundReadingClause& readingClause) {}
    virtual void visitInQueryCall(const BoundReadingClause& statement) {}
    virtual void visitLoadFrom(const BoundReadingClause& statement) {}
    void visitUpdatingClause(const BoundUpdatingClause& updatingClause);
    virtual void visitSet(const BoundUpdatingClause& updatingClause) {}
    virtual void visitDelete(const BoundUpdatingClause& updatingClause) {}
    virtual void visitInsert(const BoundUpdatingClause& updatingClause) {}
    virtual void visitMerge(const BoundUpdatingClause& updatingClause) {}

    virtual void visitProjectionBody(const BoundProjectionBody& projectionBody) {}
    virtual void visitProjectionBodyPredicate(const std::shared_ptr<Expression>& predicate) {}
};

} // namespace binder
} // namespace kuzu
