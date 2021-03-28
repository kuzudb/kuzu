#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/common/include/operations/hash_operations.h"
#include "src/planner/include/query_graph/query_rel.h"

using namespace operation;

namespace graphflow {
namespace planner {

struct Subgraph {

public:
    Subgraph() {}

    Subgraph(unordered_set<string> queryRelNames, unordered_set<string> queryNodeNames)
        : queryRelNames{move(queryRelNames)}, queryNodeNames{move(queryNodeNames)} {}

    bool containRel(string queryRelName) const {
        return end(queryRelNames) != queryRelNames.find(queryRelName);
    }

    void addRel(string queryRelName) { queryRelNames.insert(queryRelName); }

    bool containNode(string queryNodeName) const {
        return end(queryNodeNames) != queryNodeNames.find(queryNodeName);
    }

    void addNode(string queryNodeName) { queryNodeNames.insert(queryNodeName); }

    bool operator==(const Subgraph& other) const { return queryRelNames == other.queryRelNames; }

public:
    unordered_set<string> queryRelNames;
    unordered_set<string> queryNodeNames;
};

struct SubgraphJoinNodePairHasher {
    std::size_t operator()(const pair<Subgraph, string>& key) const {
        return Hash::operation(key.first.queryRelNames);
    }
};

class QueryGraph {
    friend void mergeQueryGraphs(QueryGraph& mergedQueryGraph, QueryGraph& otherQueryGraph);

public:
    uint32_t numQueryNodes() const;

    bool containsQueryNode(const string& nodeName) const;

    QueryNode* getQueryNode(const string& nodeName) const;

    void addQueryNode(unique_ptr<QueryNode> queryNode);

    uint32_t numQueryRels() const;

    bool containsQueryRel(const string& relName) const;

    vector<const QueryRel*> getQueryRels() const;

    QueryRel* getQueryRel(const string& relName) const;

    void addQueryRel(unique_ptr<QueryRel> queryRel);

    vector<pair<const QueryRel*, bool>> getConnectedQueryRelsWithDirection(
        const unordered_set<string>& queryRelNames) const;

    unordered_set<pair<Subgraph, string>, SubgraphJoinNodePairHasher> getSingleNodeJoiningSubgraph(
        const unordered_set<string>& queryRelNames, uint32_t subgraphSize) const;

    bool isConnected();

    bool operator==(const QueryGraph& other) const;

private:
    unordered_set<string> getNeighbourQueryNodeNames(const string& nodeName) const;

    unordered_set<string> getMatchedQueryNodeNames(const unordered_set<string>& relNames) const;

    unordered_set<pair<Subgraph, string>, SubgraphJoinNodePairHasher>
    createInitialSubgraphWithSingleJoinNode(const Subgraph& subgraphToExclude) const;

    unordered_set<pair<Subgraph, string>, SubgraphJoinNodePairHasher> extendSubgraphWithOneQueryRel(
        const Subgraph& subgraphToExclude,
        const pair<Subgraph, string>& subgraphWithSingleJoinNode) const;

private:
    unordered_map<string, unique_ptr<QueryNode>> nameToQueryNodeMap;
    unordered_map<string, unique_ptr<QueryRel>> nameToQueryRelMap;
};

} // namespace planner
} // namespace graphflow
