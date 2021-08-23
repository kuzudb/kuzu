#pragma once

#include "src/binder/include/query_graph/query_graph.h"
#include "src/planner/include/subplans_table.h"

namespace graphflow {
namespace planner {

/**
 * This context contains local states (states that are not shared between)
 */
class EnumeratorContext {

public:
    EnumeratorContext()
        : currentLevel{0}, subPlansTable{}, mergedQueryGraph{}, matchedQueryRels{},
          matchedQueryNodes{} {}

    void initSubPlansTable(uint32_t maxSize) { subPlansTable->init(maxSize); }

    bool isSubqueryEnumeration() const { return outerQuerySchema }

private:
    uint32_t currentLevel;
    unique_ptr<SubPlansTable> subPlansTable;

    unique_ptr<QueryGraph> mergedQueryGraph;
    // query nodes and rels matched in previous query graph
    bitset<MAX_NUM_VARIABLES> matchedQueryRels;
    bitset<MAX_NUM_VARIABLES> matchedQueryNodes;

    unique_ptr<Schema> outerQuerySchema;
};

} // namespace planner
} // namespace graphflow