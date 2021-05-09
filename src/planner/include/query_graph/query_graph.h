#pragma once

#include <bitset>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "src/expression/include/logical/logical_rel_expression.h"

using namespace graphflow::expression;

namespace graphflow {
namespace planner {

class QueryGraph;

const label_t ANY_LABEL = numeric_limits<uint32_t>::max();
const uint8_t MAX_NUM_VARIABLES = 64;

class SubqueryGraph {

public:
    explicit SubqueryGraph(const QueryGraph& queryGraph) : queryGraph{queryGraph} {}

    SubqueryGraph(const SubqueryGraph& other)
        : queryGraph{other.queryGraph}, queryNodesSelector{other.queryNodesSelector},
          queryRelsSelector{other.queryRelsSelector} {}

    void addQueryNode(uint32_t nodePos);

    void addQueryRel(uint32_t relPos);

    void addSubqueryGraph(const SubqueryGraph& other);

    bool containAllVars(unordered_set<string>& vars) const;

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
    uint32_t getNumQueryNodes() const;

    bool containsQueryNode(const string& queryNodeName) const;

    LogicalNodeExpression* getQueryNode(const string& queryNodeName) const;

    uint32_t getQueryNodePos(const string& queryNodeName) const;

    void addQueryNodeIfNotExist(shared_ptr<LogicalNodeExpression> queryNode);

    uint32_t getNumQueryRels() const;

    bool containsQueryRel(const string& queryRelName) const;

    LogicalRelExpression* getQueryRel(const string& queryRelName) const;

    uint32_t getQueryRelPos(const string& queryRelName) const;

    void addQueryRelIfNotExist(shared_ptr<LogicalRelExpression> queryRel);

    vector<tuple<uint32_t, bool, bool>> getConnectedQueryRelsWithDirection(
        const SubqueryGraph& subqueryGraph) const;

    unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
    getSingleNodeJoiningSubgraph(
        const SubqueryGraph& matchedSubqueryGraph, uint32_t subgraphSize) const;

    unordered_set<string> getNeighbourNodeNames(const string& queryNodeName) const;

    void merge(QueryGraph& other);

private:
    unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
    initSubgraphWithSingleJoinNode(const SubqueryGraph& matchedSubgraph) const;

    unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
    extendSubgraphByOneQueryRel(const SubqueryGraph& matchedSubgraph,
        const pair<SubqueryGraph, uint32_t>& subgraphWithSingleJoinNode) const;

public:
    unordered_map<string, uint32_t> queryNodeNameToPosMap;
    unordered_map<string, uint32_t> queryRelNameToPosMap;
    vector<shared_ptr<LogicalNodeExpression>> queryNodes;
    vector<shared_ptr<LogicalRelExpression>> queryRels;
};

} // namespace planner
} // namespace graphflow
