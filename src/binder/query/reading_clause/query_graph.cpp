#include "include/query_graph.h"

namespace graphflow {
namespace binder {

std::size_t SubqueryGraphHasher::operator()(const SubqueryGraph& key) const {
    if (0 == key.queryRelsSelector.count()) {
        return hash<bitset<MAX_NUM_VARIABLES>>{}(key.queryNodesSelector);
    }
    return hash<bitset<MAX_NUM_VARIABLES>>{}(key.queryRelsSelector);
}

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
        if (queryGraph.containsQueryNode(rel->getSrcNodeName())) {
            auto srcNodePos = queryGraph.getQueryNodePos(*rel->getSrcNode());
            if (!queryNodesSelector[srcNodePos]) {
                result.insert(srcNodePos);
            }
        }
        if (queryGraph.containsQueryNode(rel->getDstNodeName())) {
            auto dstNodePos = queryGraph.getQueryNodePos(*rel->getDstNode());
            if (!queryNodesSelector[dstNodePos]) {
                result.insert(dstNodePos);
            }
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
        bool isSrcNodeConnected = false;
        if (queryGraph.containsQueryNode(rel->getSrcNodeName())) {
            auto srcNodePos = queryGraph.getQueryNodePos(*rel->getSrcNode());
            isSrcNodeConnected |= queryNodesSelector[srcNodePos];
        }
        bool isDstConnected = false;
        if (queryGraph.containsQueryNode(rel->getDstNodeName())) {
            auto dstNodePos = queryGraph.getQueryNodePos(*rel->getDstNode());
            isDstConnected |= queryNodesSelector[dstNodePos];
        }
        if (isSrcNodeConnected || isDstConnected) {
            result.insert(relPos);
        }
    }
    return result;
}

unordered_set<SubqueryGraph, SubqueryGraphHasher> SubqueryGraph::getNbrSubgraphs(
    uint32_t size) const {
    auto result = getBaseNbrSubgraph();
    for (auto i = 1u; i < size; ++i) {
        unordered_set<SubqueryGraph, SubqueryGraphHasher> tmp;
        for (auto& prevNbr : result) {
            for (auto& subgraph : getNextNbrSubgraphs(prevNbr)) {
                tmp.insert(subgraph);
            }
        }
        result = move(tmp);
    }
    return result;
}

vector<uint32_t> SubqueryGraph::getConnectedNodePos(const SubqueryGraph& nbr) const {
    vector<uint32_t> result;
    for (auto& nodePos : getNodeNbrPositions()) {
        if (nbr.queryNodesSelector[nodePos]) {
            result.push_back(nodePos);
        }
    }
    for (auto& nodePos : nbr.getNodeNbrPositions()) {
        if (queryNodesSelector[nodePos]) {
            result.push_back(nodePos);
        }
    }
    return result;
}

unordered_set<SubqueryGraph, SubqueryGraphHasher> SubqueryGraph::getBaseNbrSubgraph() const {
    unordered_set<SubqueryGraph, SubqueryGraphHasher> result;
    for (auto& nodePos : getNodeNbrPositions()) {
        auto nbr = SubqueryGraph(queryGraph);
        nbr.addQueryNode(nodePos);
        result.insert(move(nbr));
    }
    for (auto& relPos : getRelNbrPositions()) {
        auto nbr = SubqueryGraph(queryGraph);
        nbr.addQueryRel(relPos);
        result.insert(move(nbr));
    }
    return result;
}

unordered_set<SubqueryGraph, SubqueryGraphHasher> SubqueryGraph::getNextNbrSubgraphs(
    const SubqueryGraph& prevNbr) const {
    unordered_set<SubqueryGraph, SubqueryGraphHasher> result;
    for (auto& nodePos : prevNbr.getNodeNbrPositions()) {
        if (queryNodesSelector[nodePos]) {
            continue;
        }
        auto nbr = prevNbr;
        nbr.addQueryNode(nodePos);
        result.insert(move(nbr));
    }
    for (auto& relPos : prevNbr.getRelNbrPositions()) {
        if (queryRelsSelector[relPos]) {
            continue;
        }
        auto nbr = prevNbr;
        nbr.addQueryRel(relPos);
        result.insert(move(nbr));
    }
    return result;
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

unique_ptr<QueryGraph> QueryGraph::copyWithoutNodes(
    const vector<shared_ptr<NodeExpression>>& nodesToExclude) const {
    auto result = make_unique<QueryGraph>();
    vector<shared_ptr<Expression>> nodesToExcludeExpressions(nodesToExclude.size());
    for (auto i = 0u; i < nodesToExclude.size(); i++) {
        nodesToExcludeExpressions[i] = nodesToExclude[i];
    }
    for (auto& queryNode : queryNodes) {
        if (ExpressionUtil::find(queryNode.get(), nodesToExcludeExpressions) == UINT32_MAX) {
            result->addQueryNode(queryNode);
        }
    }
    for (auto& queryRel : queryRels) {
        result->addQueryRel(queryRel);
    }
    return result;
}

} // namespace binder
} // namespace graphflow
