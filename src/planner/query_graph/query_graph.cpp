#include "src/planner/include/query_graph/query_graph.h"

namespace graphflow {
namespace planner {

int QueryGraph::numQueryNodes() const {
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

int QueryGraph::numQueryRels() const {
    return nameToQueryRelMap.size();
}

bool QueryGraph::containsQueryRel(const string& relName) const {
    return end(nameToQueryRelMap) != nameToQueryRelMap.find(relName);
}

QueryRel* QueryGraph::getQueryRel(const string& relName) const {
    return nameToQueryRelMap.find(relName)->second.get();
}

void QueryGraph::addQueryRel(unique_ptr<QueryRel> queryRel) {
    nameToQueryRelMap.insert({queryRel->getName(), move(queryRel)});
}

bool QueryGraph::isConnected() const {
    auto visited = unordered_set<string>();
    auto frontier = unordered_set<string>();
    auto src = begin(nameToQueryNodeMap)->first;
    frontier.insert(src);
    while (!frontier.empty()) {
        auto nextFrontier = unordered_set<string>();
        for (auto frontierIt = begin(frontier); frontierIt != end(frontier); ++frontierIt) {
            auto nbrs = getNeighbourQueryNodes(*frontierIt);
            for (auto nbrIt = begin(nbrs); nbrIt != end(nbrs); ++nbrIt) {
                if (end(visited) == visited.find((*nbrIt)->getName())) {
                    visited.insert((*nbrIt)->getName());
                    nextFrontier.insert((*nbrIt)->getName());
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
