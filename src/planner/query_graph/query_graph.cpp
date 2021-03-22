#include "src/planner/include/query_graph/query_graph.h"

namespace graphflow {
namespace planner {

uint QueryGraph::numQueryNodes() const {
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

uint QueryGraph::numQueryRels() const {
    return nameToQueryRelMap.size();
}

bool QueryGraph::containsQueryRel(const string& relName) const {
    return end(nameToQueryRelMap) != nameToQueryRelMap.find(relName);
}

vector<QueryRel*> QueryGraph::getQueryRels() const {
    vector<QueryRel*> queryRels;
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
    unordered_set<string> matchedQueryNodeNames;
    for (auto& queryRelName : queryRelNames) {
        auto queryRel = nameToQueryRelMap.at(queryRelName).get();
        matchedQueryNodeNames.insert(queryRel->getSrcNode()->getName());
        matchedQueryNodeNames.insert(queryRel->getDstNode()->getName());
    }
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

bool QueryGraph::isConnected() {
    auto visited = unordered_set<string>();
    auto frontier = unordered_set<string>();
    auto src = begin(nameToQueryNodeMap)->first;
    frontier.insert(src);
    while (!frontier.empty()) {
        auto nextFrontier = unordered_set<string>();
        for (auto& nodeInFrontier : frontier) {
            auto nbrs = getNeighbourQueryNodes(nodeInFrontier);
            for (auto& nbr : nbrs) {
                if (end(visited) == visited.find(nbr->getName())) {
                    visited.insert(nbr->getName());
                    nextFrontier.insert(nbr->getName());
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

vector<QueryNode*> QueryGraph::getNeighbourQueryNodes(const string& nodeName) const {
    auto nbrs = vector<QueryNode*>();
    for (auto it = begin(nameToQueryRelMap); it != end(nameToQueryRelMap); ++it) {
        if (it->second->getSrcNode()->getName() == nodeName) {
            nbrs.push_back(it->second->getDstNode());
        }
        if (it->second->getDstNode()->getName() == nodeName) {
            nbrs.push_back(it->second->getSrcNode());
        }
    }
    return nbrs;
}

} // namespace planner
} // namespace graphflow
