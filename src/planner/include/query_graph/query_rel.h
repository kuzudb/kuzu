#pragma once

#include "src/planner/include/query_graph/query_node.h"

namespace graphflow {
namespace planner {

class QueryRel {

public:
    QueryRel(string name, label_t label) : name{move(name)}, label{label} {}

    inline string getSrcNodeName() const { return srcNode->name; }

    inline string getDstNodeName() const { return dstNode->name; }

    bool operator==(const QueryRel& other) const {
        auto result = name == other.name && label == other.label;
        result &= *srcNode == *other.srcNode;
        result &= *dstNode == *other.dstNode;
        return result;
    }

public:
    string name;
    label_t label;
    QueryNode* srcNode;
    QueryNode* dstNode;
};

} // namespace planner
} // namespace graphflow
