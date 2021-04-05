#pragma once

#include <bitset>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/planner/include/query_graph/query_rel.h"

namespace graphflow {
namespace planner {

class QueryGraph;

const uint8_t MAX_NUM_VARIABLES = 64;

class SubqueryGraph {

public:
    explicit SubqueryGraph(const QueryGraph& queryGraph) : queryGraph{queryGraph} {}

    SubqueryGraph(const SubqueryGraph& other)
        : queryGraph{other.queryGraph}, queryNodesSelector{other.queryNodesSelector},
          queryRelsSelector{other.queryRelsSelector} {}

    void addQueryRel(uint32_t relIdx);

    void addSubqueryGraph(const SubqueryGraph& other);

    bool operator==(const SubqueryGraph& other) const;

public:
    const QueryGraph& queryGraph;
    bitset<MAX_NUM_VARIABLES> queryNodesSelector;
    bitset<MAX_NUM_VARIABLES> queryRelsSelector;
};

struct SubqueryGraphJoinNodePairHasher {
    size_t operator()(const pair<SubqueryGraph, uint32_t>& key) const {
        return hash<bitset<MAX_NUM_VARIABLES>>{}(key.first.queryRelsSelector);
    }
};

class QueryGraph {

public:
    bool containsQueryNode(const string& queryNodeName) const;

    QueryNode* getQueryNode(const string& queryNodeName) const;

    uint32_t getQueryNodeIdx(const string& queryNodeName) const;

    void addQueryNode(unique_ptr<QueryNode> queryNode);

    bool containsQueryRel(const string& queryRelName) const;

    QueryRel* getQueryRel(const string& queryRelName) const;

    void addQueryRel(unique_ptr<QueryRel> queryRel);

    vector<tuple<uint32_t, bool, bool>> getConnectedQueryRelsWithDirection(
        const SubqueryGraph& subqueryGraph) const;

    unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
    getSingleNodeJoiningSubgraph(
        const SubqueryGraph& matchedSubqueryGraph, uint32_t subgraphSize) const;

    bool isConnected() const;

    bool operator==(const QueryGraph& other) const;

private:
    unordered_set<string> getNeighbourNodeNames(const string& queryNodeName) const;

    unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
    initSubgraphWithSingleJoinNode(const SubqueryGraph& matchedSubgraph) const;

    unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
    extendSubgraphByOneQueryRel(const SubqueryGraph& matchedSubgraph,
        const pair<SubqueryGraph, uint32_t>& subgraphWithSingleJoinNode) const;

public:
    unordered_map<string, uint32_t> queryNodeNameToIdxMap;
    unordered_map<string, uint32_t> queryRelNameToIdxMap;
    vector<unique_ptr<QueryNode>> queryNodes;
    vector<unique_ptr<QueryRel>> queryRels;
};

} // namespace planner
} // namespace graphflow
