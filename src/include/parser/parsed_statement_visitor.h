#pragma once

#include "statement.h"

namespace kuzu {
namespace parser {

class SingleQuery;
class QueryPart;
class ReadingClause;
class UpdatingClause;
class WithClause;
class ReturnClause;

class StatementVisitor {
public:
    StatementVisitor() = default;
    virtual ~StatementVisitor() = default;

    void visit(const Statement& statement);

private:
    // LCOV_EXCL_START
    virtual void visitQuery(const Statement& statement);
    virtual void visitSingleQuery(const SingleQuery* singleQuery);
    virtual void visitQueryPart(const QueryPart* queryPart);
    virtual void visitReadingClause(const ReadingClause* readingClause);
    virtual void visitMatch(const ReadingClause* /*readingClause*/) {}
    virtual void visitUnwind(const ReadingClause* /*readingClause*/) {}
    virtual void visitInQueryCall(const ReadingClause* /*readingClause*/) {}
    virtual void visitLoadFrom(const ReadingClause* /*readingClause*/) {}
    virtual void visitUpdatingClause(const UpdatingClause* /*updatingClause*/);
    virtual void visitSet(const UpdatingClause* /*updatingClause*/) {}
    virtual void visitDelete(const UpdatingClause* /*updatingClause*/) {}
    virtual void visitInsert(const UpdatingClause* /*updatingClause*/) {}
    virtual void visitMerge(const UpdatingClause* /*updatingClause*/) {}
    virtual void visitWithClause(const WithClause* /*withClause*/) {}
    virtual void visitReturnClause(const ReturnClause* /*returnClause*/) {}

    virtual void visitCreateTable(const Statement& /*statement*/) {}
    virtual void visitDropTable(const Statement& /*statement*/) {}
    virtual void visitAlter(const Statement& /*statement*/) {}
    virtual void visitCopyFrom(const Statement& /*statement*/) {}
    virtual void visitCopyTo(const Statement& /*statement*/) {}
    virtual void visitStandaloneCall(const Statement& /*statement*/) {}
    virtual void visitExplain(const Statement& /*statement*/);
    virtual void visitCreateMacro(const Statement& /*statement*/) {}
    virtual void visitCommentOn(const Statement& /*statement*/) {}
    virtual void visitTransaction(const Statement& /*statement*/) {}
    // LCOV_EXCL_STOP
};

} // namespace parser
} // namespace kuzu
