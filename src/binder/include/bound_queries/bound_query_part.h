#pragma once

#include "src/binder/include/bound_statements/bound_match_statement.h"
#include "src/binder/include/bound_statements/bound_with_statement.h"

namespace graphflow {
namespace binder {

/**
 * Represents (Reading)* WITH
 */
class BoundQueryPart {

public:
    vector<unique_ptr<BoundMatchStatement>> boundMatchStatements;
    unique_ptr<BoundWithStatement> boundWithStatement;
};

} // namespace binder
} // namespace graphflow
