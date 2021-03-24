#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/planner/include/query_graph/query_rel.h"

namespace graphflow {
namespace planner {

class QueryGraph {
    friend void mergeQueryGraphs(QueryGraph& mergedQueryGraph, QueryGraph& otherQueryGraph);

public:
    uint numQueryNodes() const;

    bool containsQueryNode(const string& nodeName) const;

    QueryNode* getQueryNode(const string& nodeName) const;

    void addQueryNode(unique_ptr<QueryNode> queryNode);

    uint numQueryRels() const;

    bool containsQueryRel(const string& relName) const;

    vector<QueryRel*> getQueryRels() const;

    QueryRel* getQueryRel(const string& relName) const;

    void addQueryRel(unique_ptr<QueryRel> queryRel);

    vector<pair<const QueryRel*, bool>> getConnectedQueryRelsWithDirection(
        const unordered_set<string>& queryRelNames) const;

    bool isConnected();

    bool operator==(const QueryGraph& other) const;

private:
    vector<QueryNode*> getNeighbourQueryNodes(const string& nodeName) const;

private:
    unordered_map<string, unique_ptr<QueryNode>> nameToQueryNodeMap;
    unordered_map<string, unique_ptr<QueryRel>> nameToQueryRelMap;
};

} // namespace planner
} // namespace graphflow
