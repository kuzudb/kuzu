#pragma once

#include "src/parser/include/queries/query_part.h"

namespace graphflow {
namespace parser {

/**
 * Represents (QueryPart)* (Reading)* RETURN
 */
class SingleQuery {

public:
    vector<unique_ptr<QueryPart>> queryParts;
    vector<unique_ptr<ReadingStatement>> readingStatements;
    unique_ptr<ReturnStatement> returnStatement;
    bool enable_profile = false;
};

} // namespace parser
} // namespace graphflow
