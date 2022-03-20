#pragma once

#include <bitset>
#include <unordered_map>

#include "src/binder/expression/include/rel_expression.h"

namespace graphflow {
namespace binder {

class QueryGraph;

const label_t ANY_LABEL = numeric_limits<uint32_t>::max();
const uint8_t MAX_NUM_VARIABLES = 64;

class SubqueryGraph {

public:
    explicit SubqueryGraph(const QueryGraph& queryGraph) : queryGraph{queryGraph} {}

    SubqueryGraph(const SubqueryGraph& other)
        : queryGraph{other.queryGraph}, queryNodesSelector{other.queryNodesSelector},
          queryRelsSelector{other.queryRelsSelector} {}

    ~SubqueryGraph() = default;

    inline void addQueryNode(uint32_t nodePos) { queryNodesSelector[nodePos] = true; }

    void addQueryRel(uint32_t relPos);

    void addSubqueryGraph(const SubqueryGraph& other);

    bool containAllVariables(unordered_set<string>& variables) const;

    bool operator==(const SubqueryGraph& other) const {
        return queryRelsSelector == other.queryRelsSelector &&
               queryNodesSelector == other.queryNodesSelector;
    }

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
    QueryGraph() = default;

    QueryGraph(const QueryGraph& other)
        : queryNodeNameToPosMap{other.queryNodeNameToPosMap},
          queryRelNameToPosMap{other.queryRelNameToPosMap},
          queryNodes{other.queryNodes}, queryRels{other.queryRels} {}

    ~QueryGraph() = default;

    inline uint32_t getNumQueryNodes() const { return queryNodes.size(); }

    inline bool containsQueryNode(const string& queryNodeName) const {
        return queryNodeNameToPosMap.contains(queryNodeName);
    }

    inline NodeExpression* getQueryNode(const string& queryNodeName) const {
        return queryNodes.at(queryNodeNameToPosMap.at(queryNodeName)).get();
    }

    inline NodeExpression* getQueryNode(uint32_t nodePos) const {
        return queryNodes[nodePos].get();
    }

    inline uint32_t getQueryNodePos(const string& queryNodeName) const {
        return queryNodeNameToPosMap.at(queryNodeName);
    }

    void addQueryNode(shared_ptr<NodeExpression> queryNode);

    inline uint32_t getNumQueryRels() const { return queryRels.size(); }

    inline bool containsQueryRel(const string& queryRelName) const {
        return queryRelNameToPosMap.contains(queryRelName);
    }

    inline RelExpression* getQueryRel(const string& queryRelName) const {
        return queryRels.at(queryRelNameToPosMap.at(queryRelName)).get();
    }

    inline shared_ptr<RelExpression> getQueryRel(uint32_t relPos) const {
        return queryRels[relPos];
    }

    inline uint32_t getQueryRelPos(const string& queryRelName) const {
        return queryRelNameToPosMap.at(queryRelName);
    }

    void addQueryRel(shared_ptr<RelExpression> queryRel);

    vector<tuple<uint32_t, bool, bool>> getConnectedQueryRelsWithDirection(
        const SubqueryGraph& subqueryGraph) const;

    unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
    getSingleNodeJoiningSubgraph(
        const SubqueryGraph& matchedSubqueryGraph, uint32_t subgraphSize) const;

    unordered_set<string> getNeighbourNodeNames(const string& queryNodeName) const;

    void merge(const QueryGraph& other);

    vector<shared_ptr<Expression>> getNodeIDExpressions() const;

    inline unique_ptr<QueryGraph> copy() const { return make_unique<QueryGraph>(*this); }

private:
    unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
    initSubgraphWithSingleJoinNode(const SubqueryGraph& matchedSubgraph) const;

    unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
    extendSubgraphByOneQueryRel(const SubqueryGraph& matchedSubgraph,
        const pair<SubqueryGraph, uint32_t>& subgraphWithSingleJoinNode) const;

private:
    unordered_map<string, uint32_t> queryNodeNameToPosMap;
    unordered_map<string, uint32_t> queryRelNameToPosMap;
    vector<shared_ptr<NodeExpression>> queryNodes;
    vector<shared_ptr<RelExpression>> queryRels;
};

} // namespace binder
} // namespace graphflow
