#pragma once

#include "src/parser/include/queries/query_part.h"

namespace graphflow {
namespace parser {

/**
 * Represents (QueryPart)* (MATCH WHERE?)* RETURN
 */
class SingleQuery {

public:
    bool hasMatchStatement() const;

    MatchStatement* getFirstMatchStatement() const;

public:
    vector<unique_ptr<QueryPart>> queryParts;
    vector<unique_ptr<MatchStatement>> matchStatements;
    unique_ptr<ReturnStatement> returnStatement;
};

} // namespace parser
} // namespace graphflow
