#include "src/planner/include/query_graph/query_graph.h"

namespace graphflow {
namespace planner {

uint32_t QueryGraph::numQueryNodes() const {
    return nameToQueryNodeMap.size();
}

bool QueryGraph::containsQueryNode(const string& nodeName) const {
    return end(nameToQueryNodeMap) != nameToQueryNodeMap.find(nodeName);
}

QueryNode* QueryGraph::getQueryNode(const string& nodeName) const {
    return nameToQueryNodeMap.find(nodeName)->second.get();
}

void QueryGraph::addQueryNode(unique_ptr<QueryNode> queryNode) {
    nameToQueryNodeMap.insert({queryNode->getName(), move(queryNode)});
}

uint32_t QueryGraph::numQueryRels() const {
    return nameToQueryRelMap.size();
}

bool QueryGraph::containsQueryRel(const string& relName) const {
    return end(nameToQueryRelMap) != nameToQueryRelMap.find(relName);
}

vector<const QueryRel*> QueryGraph::getQueryRels() const {
    vector<const QueryRel*> queryRels;
    for (auto& nameAndQueryRels : nameToQueryRelMap) {
        queryRels.push_back(nameAndQueryRels.second.get());
    }
    return queryRels;
}

QueryRel* QueryGraph::getQueryRel(const string& relName) const {
    return nameToQueryRelMap.find(relName)->second.get();
}

void QueryGraph::addQueryRel(unique_ptr<QueryRel> queryRel) {
    nameToQueryRelMap.insert({queryRel->getName(), move(queryRel)});
}

vector<pair<const QueryRel*, bool>> QueryGraph::getConnectedQueryRelsWithDirection(
    const unordered_set<string>& queryRelNames) const {
    vector<pair<const QueryRel*, bool>> result;
    auto matchedQueryNodeNames = getMatchedQueryNodeNames(queryRelNames);
    for (auto& nameAndQueryRel : nameToQueryRelMap) {
        auto queryRel = nameAndQueryRel.second.get();
        auto srcNodeName = queryRel->getSrcNode()->getName();
        auto dstNodeName = queryRel->getDstNode()->getName();
        auto isSrcNodeMatched =
            end(matchedQueryNodeNames) != matchedQueryNodeNames.find(srcNodeName);
        auto isDstNodeMatched =
            end(matchedQueryNodeNames) != matchedQueryNodeNames.find(dstNodeName);
        if (isSrcNodeMatched && !isDstNodeMatched) {
            result.emplace_back(queryRel, true);
        } else if (!isSrcNodeMatched && isDstNodeMatched) {
            result.emplace_back(queryRel, false);
        }
    }
    return result;
}

unordered_set<pair<Subgraph, string>, SubgraphJoinNodePairHasher>
QueryGraph::getSingleNodeJoiningSubgraph(
    const unordered_set<string>& queryRelNames, uint32_t subgraphSize) const {
    auto subgraphToExclude = Subgraph(queryRelNames, getMatchedQueryNodeNames(queryRelNames));
    auto subgraphAndJoinNodePairs = createInitialSubgraphWithSingleJoinNode(subgraphToExclude);
    for (auto i = 1ul; i < subgraphSize; ++i) {
        auto nextSubgraphAndJoinNodePairs =
            unordered_set<pair<Subgraph, string>, SubgraphJoinNodePairHasher>();
        for (auto& subgraphAndJoinNodePair : subgraphAndJoinNodePairs) {
            auto tmp = extendSubgraphWithOneQueryRel(subgraphToExclude, subgraphAndJoinNodePair);
            nextSubgraphAndJoinNodePairs.insert(begin(tmp), end(tmp));
        }
        subgraphAndJoinNodePairs = nextSubgraphAndJoinNodePairs;
    }
    return subgraphAndJoinNodePairs;
}

bool QueryGraph::isConnected() {
    auto visited = unordered_set<string>();
    auto frontier = unordered_set<string>();
    auto src = begin(nameToQueryNodeMap)->first;
    frontier.insert(src);
    while (!frontier.empty()) {
        auto nextFrontier = unordered_set<string>();
        for (auto& nodeInFrontier : frontier) {
            auto nbrs = getNeighbourQueryNodeNames(nodeInFrontier);
            for (auto& nbr : nbrs) {
                if (end(visited) == visited.find(nbr)) {
                    visited.insert(nbr);
                    nextFrontier.insert(nbr);
                }
            }
        }
        if (visited.size() == numQueryNodes()) {
            return true;
        }
        frontier = nextFrontier;
    }
    return false;
}

bool QueryGraph::operator==(const QueryGraph& other) const {
    auto result = true;
    result &= nameToQueryNodeMap.size() == other.nameToQueryNodeMap.size();
    for (auto it = begin(nameToQueryNodeMap); it != end(nameToQueryNodeMap); ++it) {
        result &=
            other.containsQueryNode(it->first) && *it->second == *other.getQueryNode(it->first);
    }
    result &= nameToQueryRelMap.size() == other.nameToQueryRelMap.size();
    for (auto it = begin(nameToQueryRelMap); it != end(nameToQueryRelMap); ++it) {
        result &= other.containsQueryRel(it->first) && *it->second == *other.getQueryRel(it->first);
    }
    return result;
}

unordered_set<string> QueryGraph::getNeighbourQueryNodeNames(const string& nodeName) const {
    auto nbrs = unordered_set<string>();
    for (auto& nameAndQueryRel : nameToQueryRelMap) {
        if (nameAndQueryRel.second->getSrcNode()->getName() == nodeName) {
            nbrs.insert(nameAndQueryRel.second->getDstNode()->getName());
        }
        if (nameAndQueryRel.second->getDstNode()->getName() == nodeName) {
            nbrs.insert(nameAndQueryRel.second->getSrcNode()->getName());
        }
    }
    return nbrs;
}

unordered_set<string> QueryGraph::getMatchedQueryNodeNames(
    const unordered_set<string>& relNames) const {
    auto nodes = unordered_set<string>();
    for (auto& relName : relNames) {
        auto& queryRel = *nameToQueryRelMap.at(relName);
        nodes.insert(queryRel.getSrcNode()->getName());
        nodes.insert(queryRel.getDstNode()->getName());
    }
    return nodes;
}

unordered_set<pair<Subgraph, string>, SubgraphJoinNodePairHasher>
QueryGraph::createInitialSubgraphWithSingleJoinNode(const Subgraph& subgraphToExclude) const {
    auto result = unordered_set<pair<Subgraph, string>, SubgraphJoinNodePairHasher>();
    for (auto& nameAndQueryRelPair : nameToQueryRelMap) {
        auto relName = nameAndQueryRelPair.first;
        if (subgraphToExclude.containRel(relName)) {
            continue;
        }
        auto srcNodeName = nameAndQueryRelPair.second->getSrcNode()->getName();
        auto dstNodeName = nameAndQueryRelPair.second->getDstNode()->getName();
        if (subgraphToExclude.containNode(srcNodeName) &&
            !subgraphToExclude.containNode(dstNodeName)) {
            result.emplace(make_pair(Subgraph(unordered_set<string>({relName}),
                                         unordered_set<string>({srcNodeName, dstNodeName})),
                srcNodeName));
        } else if (!subgraphToExclude.containNode(srcNodeName) &&
                   subgraphToExclude.containNode(dstNodeName)) {
            result.emplace(make_pair(Subgraph(unordered_set<string>({relName}),
                                         unordered_set<string>({srcNodeName, dstNodeName})),
                dstNodeName));
        }
    }
    return result;
}

unordered_set<pair<Subgraph, string>, SubgraphJoinNodePairHasher>
QueryGraph::extendSubgraphWithOneQueryRel(const Subgraph& subgraphToExclude,
    const pair<Subgraph, string>& subgraphWithSingleJoinNode) const {
    auto result = unordered_set<pair<Subgraph, string>, SubgraphJoinNodePairHasher>();
    auto& subgraph = subgraphWithSingleJoinNode.first;
    for (auto& nameAndQueryRelPair : nameToQueryRelMap) {
        auto relName = nameAndQueryRelPair.first;
        if (subgraphToExclude.containRel(relName) || subgraph.containRel(relName)) {
            continue;
        }
        auto srcNodeName = nameAndQueryRelPair.second->getSrcNode()->getName();
        auto dstNodeName = nameAndQueryRelPair.second->getDstNode()->getName();
        if (subgraph.containNode(srcNodeName) || subgraph.containNode(dstNodeName)) {
            if (subgraphToExclude.containNode(srcNodeName) &&
                srcNodeName != subgraphWithSingleJoinNode.second) { // single join node check on src
                continue;
            }
            if (subgraphToExclude.containNode(dstNodeName) &&
                dstNodeName != subgraphWithSingleJoinNode.second) { // single join node check on dst
                continue;
            }
            auto newSubgraph = Subgraph(subgraph.queryRelNames, subgraph.queryNodeNames);
            newSubgraph.addRel(relName);
            newSubgraph.addNode(srcNodeName);
            newSubgraph.addNode(dstNodeName);
            result.emplace(make_pair(newSubgraph, subgraphWithSingleJoinNode.second));
        }
    }
    return result;
}

} // namespace planner
} // namespace graphflow
