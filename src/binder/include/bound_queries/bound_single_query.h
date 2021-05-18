#pragma once

#include "src/binder/include/bound_queries/bound_query_part.h"
#include "src/binder/include/bound_statements/bound_return_statement.h"

namespace graphflow {
namespace binder {

/**
 * Represents (QueryPart)* (MATCH WHERE?)* RETURN
 * Merge multiple match statements into single match statement
 * BoundSingleQuery may not have boundQueryParts or boundMatchStatement
 */
class BoundSingleQuery {

public:
    uint32_t getNumQueryRels() const {
        auto numQueryRels =
            boundMatchStatement ? boundMatchStatement->queryGraph->getNumQueryRels() : 0;
        for (auto& boundQueryPart : boundQueryParts) {
            numQueryRels += boundQueryPart->getNumQueryRels();
        }
        return numQueryRels;
    }

public:
    // WITH query parts
    vector<unique_ptr<BoundQueryPart>> boundQueryParts;
    unique_ptr<BoundMatchStatement> boundMatchStatement;
    unique_ptr<BoundReturnStatement> boundReturnStatement;
};

} // namespace binder
} // namespace graphflow
