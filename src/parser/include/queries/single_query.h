#pragma once

#include "src/parser/include/queries/query_part.h"

namespace graphflow {
namespace parser {

/**
 * Represents (QueryPart)* (MATCH)* RETURN
 */
class SingleQuery {

public:
    vector<unique_ptr<QueryPart>> queryParts;
    vector<unique_ptr<MatchStatement>> matchStatements;
    unique_ptr<ReturnStatement> returnStatement;
};

} // namespace parser
} // namespace graphflow
