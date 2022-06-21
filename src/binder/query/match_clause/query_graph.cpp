#include "include/query_graph.h"

namespace graphflow {
namespace binder {

bool SubqueryGraph::containAllVariables(unordered_set<string>& variables) const {
    for (auto& var : variables) {
        if (queryGraph.containsQueryNode(var) &&
            !queryNodesSelector[queryGraph.getQueryNodePos(var)]) {
            return false;
        }
        if (queryGraph.containsQueryRel(var) &&
            !queryRelsSelector[queryGraph.getQueryRelPos(var)]) {
            return false;
        }
    }
    return true;
}

unordered_set<uint32_t> SubqueryGraph::getNodeNbrPositions() const {
    unordered_set<uint32_t> result;
    for (auto relPos = 0u; relPos < queryGraph.getNumQueryRels(); ++relPos) {
        if (!queryRelsSelector[relPos]) { // rel not in subgraph, no need to check
            continue;
        }
        auto rel = queryGraph.getQueryRel(relPos);
        auto srcNodePos = queryGraph.getQueryNodePos(*rel->getSrcNode());
        if (!queryNodesSelector[srcNodePos]) {
            result.insert(srcNodePos);
        }
        auto dstNodePos = queryGraph.getQueryNodePos(*rel->getDstNode());
        if (!queryNodesSelector[dstNodePos]) {
            result.insert(dstNodePos);
        }
    }
    return result;
}

unordered_set<uint32_t> SubqueryGraph::getRelNbrPositions() const {
    unordered_set<uint32_t> result;
    for (auto relPos = 0u; relPos < queryGraph.getNumQueryRels(); ++relPos) {
        if (queryRelsSelector[relPos]) { // rel already in subgraph, cannot be rel neighbour
            continue;
        }
        auto rel = queryGraph.getQueryRel(relPos);
        auto srcNodePos = queryGraph.getQueryNodePos(*rel->getSrcNode());
        auto dstNodePos = queryGraph.getQueryNodePos(*rel->getDstNode());
        if (queryNodesSelector[srcNodePos] || queryNodesSelector[dstNodePos]) {
            result.insert(relPos);
        }
    }
    return result;
}

bool SubqueryGraph::isSrcConnected(uint32_t relPos) const {
    assert(!queryRelsSelector[relPos]);
    auto srcNodePos = queryGraph.getQueryNodePos(*queryGraph.getQueryRel(relPos)->getSrcNode());
    return queryNodesSelector[srcNodePos];
}

bool SubqueryGraph::isDstConnected(uint32_t relPos) const {
    assert(!queryRelsSelector[relPos]);
    auto dstNodePos = queryGraph.getQueryNodePos(*queryGraph.getQueryRel(relPos)->getDstNode());
    return queryNodesSelector[dstNodePos];
}

bool SubqueryGraph::isClosingRel(uint32_t relPos) const {
    unordered_set<uint32_t> matchedNodePositions;
    for (auto i = 0u; i < queryGraph.getNumQueryRels(); ++i) {
        if (!queryRelsSelector[i]) {
            continue;
        }
        auto matchedRel = queryGraph.getQueryRel(i);
        auto srcNodePos = queryGraph.getQueryNodePos(*matchedRel->getSrcNode());
        auto dstNodePos = queryGraph.getQueryNodePos(*matchedRel->getDstNode());
        matchedNodePositions.insert(srcNodePos);
        matchedNodePositions.insert(dstNodePos);
    }
    auto rel = queryGraph.getQueryRel(relPos);
    auto srcNodePos = queryGraph.getQueryNodePos(*rel->getSrcNode());
    auto dstNodePos = queryGraph.getQueryNodePos(*rel->getDstNode());
    return matchedNodePositions.contains(srcNodePos) && matchedNodePositions.contains(dstNodePos);
}

void QueryGraph::addQueryNode(shared_ptr<NodeExpression> queryNode) {
    // Note that a node may be added multiple times. We should only keep one of it.
    // E.g. MATCH (a:person)-[:knows]->(b:person), (a)-[:knows]->(c:person)
    // a will be added twice during binding
    if (containsQueryNode(queryNode->getUniqueName())) {
        return;
    }
    queryNodeNameToPosMap.insert({queryNode->getUniqueName(), queryNodes.size()});
    queryNodes.push_back(queryNode);
}

void QueryGraph::addQueryRel(shared_ptr<RelExpression> queryRel) {
    assert(!containsQueryRel(queryRel->getUniqueName()));
    queryRelNameToPosMap.insert({queryRel->getUniqueName(), queryRels.size()});
    queryRels.push_back(queryRel);
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
        subgraphAndJoinNodePairs = move(nextSubgraphAndJoinNodePairs);
    }
    return subgraphAndJoinNodePairs;
}

unordered_set<string> QueryGraph::getNeighbourNodeNames(const string& queryNodeName) const {
    auto nbrs = unordered_set<string>();
    for (auto& rel : queryRels) {
        if (rel->getSrcNodeName() == queryNodeName) {
            if (end(queryNodeNameToPosMap) != queryNodeNameToPosMap.find(rel->getDstNodeName())) {
                nbrs.insert(rel->getDstNodeName());
            }
        }
        if (rel->getDstNodeName() == queryNodeName) {
            if (end(queryNodeNameToPosMap) != queryNodeNameToPosMap.find(rel->getSrcNodeName())) {
                nbrs.insert(rel->getSrcNodeName());
            }
        }
    }
    return nbrs;
}

void QueryGraph::merge(const QueryGraph& other) {
    for (auto& otherNode : other.queryNodes) {
        addQueryNode(otherNode);
    }
    for (auto& otherRel : other.queryRels) {
        addQueryRel(otherRel);
    }
}

vector<shared_ptr<Expression>> QueryGraph::getNodeIDExpressions() const {
    vector<shared_ptr<Expression>> result;
    for (auto& queryNode : queryNodes) {
        result.push_back(queryNode->getNodeIDPropertyExpression());
    }
    return result;
}

unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
QueryGraph::initSubgraphWithSingleJoinNode(const SubqueryGraph& matchedSubgraph) const {
    auto result = unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>();
    for (auto relPos = 0u; relPos < queryRels.size(); ++relPos) {
        if (matchedSubgraph.queryRelsSelector[relPos]) {
            continue;
        }
        auto srcNodePos = queryNodeNameToPosMap.at(queryRels[relPos]->getSrcNodeName());
        auto dstNodePos = queryNodeNameToPosMap.at(queryRels[relPos]->getDstNodeName());
        // check join on single node
        if (matchedSubgraph.queryNodesSelector[srcNodePos] &&
            !matchedSubgraph.queryNodesSelector[dstNodePos]) {
            auto subgraph = SubqueryGraph(*this);
            subgraph.addQueryRel(relPos);
            result.emplace(make_pair(subgraph, srcNodePos));
        } else if (!matchedSubgraph.queryNodesSelector[srcNodePos] &&
                   matchedSubgraph.queryNodesSelector[dstNodePos]) {
            auto subgraph = SubqueryGraph(*this);
            subgraph.addQueryRel(relPos);
            result.emplace(make_pair(subgraph, dstNodePos));
        }
    }
    return result;
}

unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>
QueryGraph::extendSubgraphByOneQueryRel(const SubqueryGraph& matchedSubgraph,
    const pair<SubqueryGraph, uint32_t>& subgraphWithSingleJoinNode) const {
    auto result = unordered_set<pair<SubqueryGraph, uint32_t>, SubqueryGraphJoinNodePairHasher>();
    auto& subgraph = subgraphWithSingleJoinNode.first;
    for (auto relPos = 0u; relPos < queryRels.size(); ++relPos) {
        if (matchedSubgraph.queryRelsSelector[relPos] || subgraph.queryRelsSelector[relPos]) {
            continue;
        }
        auto srcNodePos = queryNodeNameToPosMap.at(queryRels.at(relPos)->getSrcNodeName());
        auto dstNodePos = queryNodeNameToPosMap.at(queryRels.at(relPos)->getDstNodeName());
        if (subgraph.queryNodesSelector[srcNodePos] || subgraph.queryNodesSelector[dstNodePos]) {
            if (matchedSubgraph.queryNodesSelector[srcNodePos] &&
                srcNodePos != subgraphWithSingleJoinNode.second) { // single join node check on src
                continue;
            }
            if (matchedSubgraph.queryNodesSelector[dstNodePos] &&
                dstNodePos != subgraphWithSingleJoinNode.second) { // single join node check on dst
                continue;
            }
            auto newSubgraph = subgraph;
            newSubgraph.addQueryRel(relPos);
            result.emplace(make_pair(newSubgraph, subgraphWithSingleJoinNode.second));
        }
    }
    return result;
}

} // namespace binder
} // namespace graphflow
