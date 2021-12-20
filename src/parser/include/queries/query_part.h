#pragma once

#include <cassert>

#include "src/parser/include/statements/match_statement.h"
#include "src/parser/include/statements/with_statement.h"

namespace graphflow {
namespace parser {

/**
 * Represents (MATCH WHERE?)* WITH WHERE?
 */
class QueryPart {

public:
    explicit QueryPart(unique_ptr<WithStatement> withStatement)
        : withStatement{move(withStatement)} {}

    inline bool hasMatchStatement() const { return !matchStatements.empty(); }

    inline MatchStatement* getFirstMatchStatement() const {
        assert(hasMatchStatement());
        return matchStatements[0].get();
    }

public:
    vector<unique_ptr<MatchStatement>> matchStatements;
    unique_ptr<WithStatement> withStatement;
};

} // namespace parser
} // namespace graphflow
