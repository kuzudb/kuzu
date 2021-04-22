#pragma once

#include <unordered_set>

#include "src/planner/include/query_graph/query_node.h"

namespace graphflow {
namespace planner {

class QueryRel {

public:
    QueryRel(string name, label_t label) : name{move(name)}, label{label} {}

    inline string getSrcNodeName() const { return srcNode->name; }

    inline string getDstNodeName() const { return dstNode->name; }

public:
    string name;
    label_t label;
    QueryNode* srcNode;
    QueryNode* dstNode;
};

} // namespace planner
} // namespace graphflow
