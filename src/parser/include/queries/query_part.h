#pragma once

#include "src/parser/include/statements/reading_statement.h"
#include "src/parser/include/statements/with_statement.h"

namespace graphflow {
namespace parser {

/**
 * Represents (Reading)* WITH WHERE?
 */
class QueryPart {

public:
    explicit QueryPart(unique_ptr<WithStatement> withStatement)
        : withStatement{move(withStatement)} {}

public:
    vector<unique_ptr<ReadingStatement>> readingStatements;
    unique_ptr<WithStatement> withStatement;
};

} // namespace parser
} // namespace graphflow
