#pragma once

#include "binder/bound_statement_visitor.h"

namespace kuzu {
namespace binder {

// WithClauseProjectionRewriter first analyze the properties need to be scanned for each query. And
// then rewrite node/rel expression in WITH clause as their properties. So We avoid eagerly evaluate
// node/rel in WITH clause projection. E.g.
// MATCH (a) WITH a MATCH (a)->(b);
// will be rewritten as
// MATCH (a) WITH a._id MATCH (a)->(b);
// See bind_projection_clause.cpp for more details.
class WithClauseProjectionRewriter : public BoundStatementVisitor {
public:
    void visitSingleQuery(const NormalizedSingleQuery& singleQuery) override;
};

} // namespace binder
} // namespace kuzu
