#pragma once

#include "src/binder/include/bound_statements/bound_reading_statement.h"
#include "src/binder/include/bound_statements/bound_with_statement.h"

namespace graphflow {
namespace binder {

/**
 * Represents (Reading)* WITH
 * All match statements are merged as single match statement and appended at the end.
 * Because LOAD CSV doesn't depend on the runtime output of MATCH, we push LOAD CSV to the beginning
 * Eg. MATCH + LOAD CSV + MATCH + MATCH -> LOAD CSV + MATCH (merged)
 */
class BoundQueryPart {

public:
    uint32_t getNumQueryRels() const;

public:
    vector<unique_ptr<BoundReadingStatement>> boundReadingStatements;
    unique_ptr<BoundWithStatement> boundWithStatement;
};

} // namespace binder
} // namespace graphflow
