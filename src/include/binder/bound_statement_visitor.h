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
    void visitUnsafe(BoundStatement& statement);

    virtual void visitSingleQuery(const NormalizedSingleQuery& singleQuery);

protected:
    virtual void visitCreateTable(const BoundStatement&) {}
    virtual void visitDropTable(const BoundStatement&) {}
    virtual void visitAlter(const BoundStatement&) {}
    virtual void visitCopyFrom(const BoundStatement&) {}
    virtual void visitCopyTo(const BoundStatement&) {}
    virtual void visitExportDatabase(const BoundStatement&) {}
    virtual void visitStandaloneCall(const BoundStatement&) {}
    virtual void visitCommentOn(const BoundStatement&) {}
    virtual void visitExplain(const BoundStatement&);
    virtual void visitCreateMacro(const BoundStatement&) {}
    virtual void visitTransaction(const BoundStatement&) {}
    virtual void visitExtension(const BoundStatement&) {}

    virtual void visitRegularQuery(const BoundStatement& statement);
    virtual void visitRegularQueryUnsafe(BoundStatement& statement);
    virtual void visitSingleQueryUnsafe(NormalizedSingleQuery&) {}
    virtual void visitQueryPart(const NormalizedQueryPart& queryPart);
    void visitReadingClause(const BoundReadingClause& readingClause);
    virtual void visitMatch(const BoundReadingClause& /*readingClause*/) {}
    virtual void visitUnwind(const BoundReadingClause& /*readingClause*/) {}
    virtual void visitInQueryCall(const BoundReadingClause& /*statement*/) {}
    virtual void visitLoadFrom(const BoundReadingClause& /*statement*/) {}
    void visitUpdatingClause(const BoundUpdatingClause& updatingClause);
    virtual void visitSet(const BoundUpdatingClause& /*updatingClause*/) {}
    virtual void visitDelete(const BoundUpdatingClause& /* updatingClause*/) {}
    virtual void visitInsert(const BoundUpdatingClause& /* updatingClause*/) {}
    virtual void visitMerge(const BoundUpdatingClause& /* updatingClause*/) {}

    virtual void visitProjectionBody(const BoundProjectionBody& /* projectionBody*/) {}
    virtual void visitProjectionBodyPredicate(const std::shared_ptr<Expression>& /* predicate*/) {}
};

} // namespace binder
} // namespace kuzu
