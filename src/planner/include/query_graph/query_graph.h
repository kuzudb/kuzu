#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/planner/include/query_graph/query_rel.h"

namespace graphflow {
namespace planner {

class QueryGraph {

public:
    int numQueryNodes() const;

    bool containsQueryNode(const string& nodeName) const;

    QueryNode* getQueryNode(const string& nodeName) const;

    void addQueryNode(unique_ptr<QueryNode> queryNode);

    int numQueryRels() const;

    bool containsQueryRel(const string& relName) const;

    QueryRel* getQueryRel(const string& relName) const;

    void addQueryRel(unique_ptr<QueryRel> queryRel);

    bool isConnected() const;

    bool operator==(const QueryGraph& other) const;

private:
    vector<QueryNode*> getNeighbourQueryNodes(const string& nodeName) const;

private:
    unordered_map<string, unique_ptr<QueryNode>> nameToQueryNodeMap;
    unordered_map<string, unique_ptr<QueryRel>> nameToQueryRelMap;
};

} // namespace planner
} // namespace graphflow
