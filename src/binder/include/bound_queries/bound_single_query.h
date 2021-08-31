#pragma once

#include "src/binder/include/bound_queries/bound_query_part.h"
#include "src/binder/include/bound_statements/bound_return_statement.h"

namespace graphflow {
namespace binder {

/**
 * Represents (QueryPart)* (Reading)* RETURN
 */
class BoundSingleQuery {

public:
    // WITH query parts
    vector<unique_ptr<BoundQueryPart>> boundQueryParts;
    vector<unique_ptr<BoundReadingStatement>> boundReadingStatements;
    unique_ptr<BoundReturnStatement> boundReturnStatement;
};

} // namespace binder
} // namespace graphflow
