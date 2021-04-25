#pragma once

#include "src/planner/include/bound_statements/bound_match_statement.h"
#include "src/planner/include/bound_statements/bound_with_statement.h"

using namespace graphflow::expression;

namespace graphflow {
namespace planner {

/**
 * Represents (MATCH WHERE?)* WITH
 * Merge multiple match statements as a single match statement
 * BoundQueryPart may not have boundMatchStatement
 */
class BoundQueryPart {

public:
    uint32_t getNumQueryRels() const {
        return boundMatchStatement ? boundMatchStatement->queryGraph->getNumQueryRels() : 0;
    }

public:
    unique_ptr<BoundMatchStatement> boundMatchStatement;
    unique_ptr<BoundWithStatement> boundWithStatement;
};

} // namespace planner
} // namespace graphflow
