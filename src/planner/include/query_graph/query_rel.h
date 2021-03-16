#pragma once

#include "src/planner/include/query_graph/query_node.h"

namespace graphflow {
namespace planner {

class QueryRel {

public:
    QueryRel(string name, int label) : name{move(name)}, label{label} {}

    string getName() const { return name; }

    int getLabel() const { return label; }

    QueryNode* getSrcNode() const { return srcNode; }

    QueryNode* getDstNode() const { return dstNode; }

    void setSrcNode(QueryNode* queryNode) { srcNode = queryNode; }

    void setDstNode(QueryNode* queryNode) { dstNode = queryNode; }

    bool operator==(const QueryRel& other) const {
        auto result = name == other.name && label == other.label;
        result &= *srcNode == *other.srcNode;
        result &= *dstNode == *other.dstNode;
        return result;
    }

private:
    string name;
    int label;
    QueryNode* srcNode;
    QueryNode* dstNode;
};

} // namespace planner
} // namespace graphflow
