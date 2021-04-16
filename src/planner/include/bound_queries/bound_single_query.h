#pragma once

#include "src/planner/include/bound_queries/bound_query_part.h"
#include "src/planner/include/bound_statements/bound_return_statement.h"

namespace graphflow {
namespace planner {

/**
 * Represents (QueryPart)* (MATCH WHERE?)* RETURN
 * Merge multiple match statements into single match statement
 * BoundSingleQuery may not have boundQueryParts or boundMatchStatement
 */
class BoundSingleQuery {

public:
    // WITH query parts
    vector<unique_ptr<BoundQueryPart>> boundQueryParts;
    unique_ptr<BoundMatchStatement> boundMatchStatement;
    unique_ptr<BoundReturnStatement> boundReturnStatement;
};

} // namespace planner
} // namespace graphflow
