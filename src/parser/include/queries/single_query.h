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
    // If explain is enabled, we do not execute query but return physical plan only
    bool enable_explain = false;
    bool enable_profile = false;
};

} // namespace parser
} // namespace graphflow
