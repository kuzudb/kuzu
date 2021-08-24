#pragma once

#include "src/binder/include/bound_queries/bound_query_part.h"
#include "src/binder/include/bound_statements/bound_return_statement.h"

namespace graphflow {
namespace binder {

/**
 * Represents (QueryPart)* (Reading)* RETURN
 * Similar to BoundQueryPart, all match statements in the reading statements are merged as single
 * match statement and appended at the end. See bound_query_part.h for more.
 */
class BoundSingleQuery {

public:
    uint32_t getNumQueryRels() const;

    vector<const Expression*> getIncludedVariables() const;

public:
    // WITH query parts
    vector<unique_ptr<BoundQueryPart>> boundQueryParts;
    vector<unique_ptr<BoundReadingStatement>> boundReadingStatements;
    unique_ptr<BoundReturnStatement> boundReturnStatement;
};

} // namespace binder
} // namespace graphflow
