#include "src/planner/include/query_graph/query_graph.h"

namespace graphflow {
namespace planner {

void SubqueryGraph::addQueryRel(uint32_t relIdx) {
    queryRelsSelector[relIdx] = true;
    queryNodesSelector[queryGraph.getQueryNodeIdx(queryGraph.queryRels[relIdx]->getSrcNodeName())] =
        true;
    queryNodesSelector[queryGraph.getQueryNodeIdx(queryGraph.queryRels[relIdx]->getDstNodeName())] =
        true;
}

void SubqueryGraph::addSubqueryGraph(const SubqueryGraph& other) {
    queryRelsSelector |= other.queryRelsSelector;
    queryNodesSelector |= other.queryNodesSelector;
}

bool SubqueryGraph::operator==(const SubqueryGraph& other) const {
    return queryRelsSelector == other.queryRelsSelector;
}

bool QueryGraph::containsQueryNode(const string& queryNodeName) const {
    return end(queryNodeNameToIdxMap) != queryNodeNameToIdxMap.find(queryNodeName);
}

QueryNode* QueryGraph::getQueryNode(const string& queryNodeName) const {
    return queryNodes.at(queryNodeNameToIdxMap.at(queryNodeName)).get();
}

uint32_t QueryGraph::getQueryNodeIdx(const string& queryNodeName) const {
    return queryNodeNameToIdxMap.at(queryNodeName);
}

void QueryGraph::addQueryNode(unique_ptr<QueryNode> queryNode) {
    queryNodeNameToIdxMap.insert({queryNode->name, queryNodes.size()});
    queryNodes.push_back(move(queryNode));
}

bool QueryGraph::containsQueryRel(const string& queryRelName) const {
    return end(queryRelNameToIdxMap) != queryRelNameToIdxMap.find(queryRelName);
}

QueryRel* QueryGraph::getQueryRel(const string& queryRelName) const {
    return queryRels.at(queryRelNameToIdxMap.at(queryRelName)).get();
}

void QueryGraph::addQueryRel(unique_ptr<QueryRel> queryRel) {
    queryRelNameToIdxMap.insert({queryRel->name, queryRels.size()});
    queryRels.push_back(move(queryRel));
}

vector<tuple<uint32_t, bool, bool>> QueryGraph::getConnectedQueryRelsWithDirection(
    const SubqueryGraph& subqueryGraph) const {
    vector<tuple<uint32_t, bool, bool>> result;
    for (auto relIdx = 0u; relIdx < queryRels.size(); ++relIdx) {
        if (subqueryGraph.queryRelsSelector[relIdx]) {
            continue;
        }
        auto srcNodeIdx = queryNodeNameToIdxMap.at(queryRels.at(relIdx)->getSrcNodeName());
        auto dstNodeIdx = queryNodeNameToIdxMap.at(queryRels.at(relIdx)->getDstNodeName());
        if (subqueryGraph.queryNodesSelector[srcNodeIdx] ||
            subqueryGraph.queryNodesSelector[dstNodeIdx]) {
            result.emplace_back(make_tuple(relIdx, subqueryGraph.queryNodesSelector[srcNodeIdx],
                subqueryGraph.queryNodesSelector[dstNodeIdx]));
        }
    }
    return result;
}

unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
QueryGraph::getSingleNodeJoiningSubgraph(
    const SubqueryGraph& matchedSubqueryGraph, uint32_t subgraphSize) const {
    auto subgraphAndJoinNodePairs = initSubgraphWithSingleJoinNode(matchedSubqueryGraph);
    for (auto i = 1ul; i < subgraphSize; ++i) {
        auto nextSubgraphAndJoinNodePairs =
            unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>();
        for (auto& subgraphAndJoinNodePair : subgraphAndJoinNodePairs) {
            auto tmp = extendSubgraphByOneQueryRel(matchedSubqueryGraph, subgraphAndJoinNodePair);
            nextSubgraphAndJoinNodePairs.insert(begin(tmp), end(tmp));
        }
        subgraphAndJoinNodePairs = nextSubgraphAndJoinNodePairs;
    }
    return subgraphAndJoinNodePairs;
}

bool QueryGraph::isConnected() const {
    auto visited = unordered_set<string>();
    auto frontier = unordered_set<string>();
    auto src = queryNodes.at(0)->name;
    frontier.insert(src);
    visited.insert(src);
    while (!frontier.empty()) {
        auto nextFrontier = unordered_set<string>();
        for (auto& nodeInFrontier : frontier) {
            auto nbrs = getNeighbourNodeNames(nodeInFrontier);
            for (auto& nbr : nbrs) {
                if (end(visited) == visited.find(nbr)) {
                    visited.insert(nbr);
                    nextFrontier.insert(nbr);
                }
            }
        }
        if (visited.size() == queryNodes.size()) {
            return true;
        }
        frontier = nextFrontier;
    }
    return false;
}

bool QueryGraph::operator==(const QueryGraph& other) const {
    auto result =
        queryNodes.size() == other.queryNodes.size() && queryRels.size() == other.queryRels.size();
    for (auto i = 0u; i < queryNodes.size(); ++i) {
        result &= *queryNodes.at(i) == *other.queryNodes.at(i);
    }
    for (auto i = 0u; i < queryRels.size(); ++i) {
        result &= *queryRels.at(i) == *other.queryRels.at(i);
    }
    return result;
}

unordered_set<string> QueryGraph::getNeighbourNodeNames(const string& queryNodeName) const {
    auto nbrs = unordered_set<string>();
    for (auto& rel : queryRels) {
        if (getQueryNode(rel->getSrcNodeName())->name == queryNodeName) {
            nbrs.insert(rel->getDstNodeName());
        }
        if (getQueryNode(rel->getDstNodeName())->name == queryNodeName) {
            nbrs.insert(rel->getSrcNodeName());
        }
    }
    return nbrs;
}

unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
QueryGraph::initSubgraphWithSingleJoinNode(const SubqueryGraph& matchedSubgraph) const {
    auto result = unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>();
    for (auto relIdx = 0u; relIdx < queryRels.size(); ++relIdx) {
        if (matchedSubgraph.queryRelsSelector[relIdx]) {
            continue;
        }
        auto srcNodeIdx = queryNodeNameToIdxMap.at(queryRels[relIdx]->getSrcNodeName());
        auto dstNodeIdx = queryNodeNameToIdxMap.at(queryRels[relIdx]->getDstNodeName());
        // check join on single node
        if (matchedSubgraph.queryNodesSelector[srcNodeIdx] ||
            !matchedSubgraph.queryNodesSelector[dstNodeIdx]) {
            auto subgraph = SubqueryGraph(*this);
            subgraph.addQueryRel(relIdx);
            result.emplace(make_pair(subgraph, srcNodeIdx));
        } else if (!matchedSubgraph.queryNodesSelector[srcNodeIdx] &&
                   matchedSubgraph.queryNodesSelector[dstNodeIdx]) {
            auto subgraph = SubqueryGraph(*this);
            subgraph.addQueryRel(relIdx);
            result.emplace(make_pair(subgraph, dstNodeIdx));
        }
    }
    return result;
}

unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
QueryGraph::extendSubgraphByOneQueryRel(const SubqueryGraph& matchedSubgraph,
    const pair<SubqueryGraph, uint32_t>& subgraphWithSingleJoinNode) const {
    auto result = unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>();
    auto& subgraph = subgraphWithSingleJoinNode.first;
    for (auto relIdx = 0u; relIdx < queryRels.size(); ++relIdx) {
        if (matchedSubgraph.queryRelsSelector[relIdx] || subgraph.queryRelsSelector[relIdx]) {
            continue;
        }
        auto srcNodeIdx = queryNodeNameToIdxMap.at(queryRels.at(relIdx)->getSrcNodeName());
        auto dstNodeIdx = queryNodeNameToIdxMap.at(queryRels.at(relIdx)->getDstNodeName());
        if (subgraph.queryNodesSelector[srcNodeIdx] || subgraph.queryNodesSelector[dstNodeIdx]) {
            if (matchedSubgraph.queryNodesSelector[srcNodeIdx] &&
                srcNodeIdx != subgraphWithSingleJoinNode.second) { // single join node check on src
                continue;
            }
            if (matchedSubgraph.queryNodesSelector[dstNodeIdx] &&
                dstNodeIdx != subgraphWithSingleJoinNode.second) { // single join node check on dst
                continue;
            }
            auto newSubgraph = subgraph;
            newSubgraph.addQueryRel(relIdx);
            result.emplace(make_pair(newSubgraph, subgraphWithSingleJoinNode.second));
        }
    }
    return result;
}

} // namespace planner
} // namespace graphflow
