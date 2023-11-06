#include "binder/query/query_graph.h"

#include "binder/expression_visitor.h"

namespace kuzu {
namespace binder {

std::size_t SubqueryGraphHasher::operator()(const SubqueryGraph& key) const {
    if (0 == key.queryRelsSelector.count()) {
        return std::hash<std::bitset<MAX_NUM_QUERY_VARIABLES>>{}(key.queryNodesSelector);
    }
    return std::hash<std::bitset<MAX_NUM_QUERY_VARIABLES>>{}(key.queryRelsSelector);
}

bool SubqueryGraph::containAllVariables(std::unordered_set<std::string>& variables) const {
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

std::unordered_set<uint32_t> SubqueryGraph::getNodeNbrPositions() const {
    std::unordered_set<uint32_t> result;
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

std::unordered_set<uint32_t> SubqueryGraph::getRelNbrPositions() const {
    std::unordered_set<uint32_t> result;
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

subquery_graph_set_t SubqueryGraph::getNbrSubgraphs(uint32_t size) const {
    auto result = getBaseNbrSubgraph();
    for (auto i = 1u; i < size; ++i) {
        std::unordered_set<SubqueryGraph, SubqueryGraphHasher> tmp;
        for (auto& prevNbr : result) {
            for (auto& subgraph : getNextNbrSubgraphs(prevNbr)) {
                tmp.insert(subgraph);
            }
        }
        result = std::move(tmp);
    }
    return result;
}

std::vector<uint32_t> SubqueryGraph::getConnectedNodePos(const SubqueryGraph& nbr) const {
    std::vector<uint32_t> result;
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

std::unordered_set<uint32_t> SubqueryGraph::getNodePositionsIgnoringNodeSelector() const {
    std::unordered_set<uint32_t> result;
    for (auto nodePos = 0u; nodePos < queryGraph.getNumQueryNodes(); ++nodePos) {
        if (queryNodesSelector[nodePos]) {
            result.insert(nodePos);
        }
    }
    for (auto relPos = 0u; relPos < queryGraph.getNumQueryRels(); ++relPos) {
        auto rel = queryGraph.getQueryRel(relPos);
        if (queryRelsSelector[relPos]) {
            result.insert(queryGraph.getQueryNodePos(rel->getSrcNodeName()));
            result.insert(queryGraph.getQueryNodePos(rel->getDstNodeName()));
        }
    }
    return result;
}

subquery_graph_set_t SubqueryGraph::getBaseNbrSubgraph() const {
    subquery_graph_set_t result;
    for (auto& nodePos : getNodeNbrPositions()) {
        auto nbr = SubqueryGraph(queryGraph);
        nbr.addQueryNode(nodePos);
        result.insert(std::move(nbr));
    }
    for (auto& relPos : getRelNbrPositions()) {
        auto nbr = SubqueryGraph(queryGraph);
        nbr.addQueryRel(relPos);
        result.insert(std::move(nbr));
    }
    return result;
}

subquery_graph_set_t SubqueryGraph::getNextNbrSubgraphs(const SubqueryGraph& prevNbr) const {
    subquery_graph_set_t result;
    for (auto& nodePos : prevNbr.getNodeNbrPositions()) {
        if (queryNodesSelector[nodePos]) {
            continue;
        }
        auto nbr = prevNbr;
        nbr.addQueryNode(nodePos);
        result.insert(std::move(nbr));
    }
    for (auto& relPos : prevNbr.getRelNbrPositions()) {
        if (queryRelsSelector[relPos]) {
            continue;
        }
        auto nbr = prevNbr;
        nbr.addQueryRel(relPos);
        result.insert(std::move(nbr));
    }
    return result;
}

void QueryGraph::addQueryNode(std::shared_ptr<NodeExpression> queryNode) {
    // Note that a node may be added multiple times. We should only keep one of it.
    // E.g. MATCH (a:person)-[:knows]->(b:person), (a)-[:knows]->(c:person)
    // a will be added twice during binding
    if (containsQueryNode(queryNode->getUniqueName())) {
        return;
    }
    queryNodeNameToPosMap.insert({queryNode->getUniqueName(), queryNodes.size()});
    queryNodes.push_back(queryNode);
}

void QueryGraph::addQueryRel(std::shared_ptr<RelExpression> queryRel) {
    KU_ASSERT(!containsQueryRel(queryRel->getUniqueName()));
    queryRelNameToPosMap.insert({queryRel->getUniqueName(), queryRels.size()});
    queryRels.push_back(queryRel);
}

void QueryGraph::merge(const QueryGraph& other) {
    for (auto& otherNode : other.queryNodes) {
        addQueryNode(otherNode);
    }
    for (auto& otherRel : other.queryRels) {
        addQueryRel(otherRel);
    }
}

bool QueryGraph::canProjectExpression(const std::shared_ptr<Expression>& expression) const {
    auto expressionCollector = std::make_unique<ExpressionCollector>();
    for (auto& variable : expressionCollector->getDependentVariableNames(expression)) {
        if (!containsQueryNode(variable) && !containsQueryRel(variable)) {
            return false;
        }
    }
    return true;
}

bool QueryGraph::isConnected(const QueryGraph& other) {
    for (auto& queryNode : queryNodes) {
        if (other.containsQueryNode(queryNode->getUniqueName())) {
            return true;
        }
    }
    return false;
}

void QueryGraphCollection::addAndMergeQueryGraphIfConnected(
    std::unique_ptr<QueryGraph> queryGraphToAdd) {
    bool isMerged = false;
    for (auto& queryGraph : queryGraphs) {
        if (queryGraph->isConnected(*queryGraphToAdd)) {
            queryGraph->merge(*queryGraphToAdd);
            isMerged = true;
        }
    }
    if (!isMerged) {
        queryGraphs.push_back(std::move(queryGraphToAdd));
    }
}

std::vector<std::shared_ptr<NodeExpression>> QueryGraphCollection::getQueryNodes() const {
    std::vector<std::shared_ptr<NodeExpression>> result;
    for (auto& queryGraph : queryGraphs) {
        for (auto& node : queryGraph->getQueryNodes()) {
            result.push_back(node);
        }
    }
    return result;
}

std::vector<std::shared_ptr<RelExpression>> QueryGraphCollection::getQueryRels() const {
    std::vector<std::shared_ptr<RelExpression>> result;
    for (auto& queryGraph : queryGraphs) {
        for (auto& rel : queryGraph->getQueryRels()) {
            result.push_back(rel);
        }
    }
    return result;
}

std::unique_ptr<QueryGraphCollection> QueryGraphCollection::copy() const {
    auto result = std::make_unique<QueryGraphCollection>();
    for (auto& queryGraph : queryGraphs) {
        result->queryGraphs.push_back(queryGraph->copy());
    }
    return result;
}

void PropertyKeyValCollection::addKeyVal(
    std::shared_ptr<Expression> variable, const std::string& propertyName, expression_pair keyVal) {
    if (!propertyKeyValMap.contains(variable)) {
        propertyKeyValMap.insert({variable, std::unordered_map<std::string, expression_pair>{}});
    }
    propertyKeyValMap.at(variable).insert({propertyName, std::move(keyVal)});
}

std::vector<expression_pair> PropertyKeyValCollection::getKeyVals() const {
    std::vector<expression_pair> result;
    for (auto& [_, keyVals] : propertyKeyValMap) {
        for (auto& [_, keyVal] : keyVals) {
            result.push_back(keyVal);
        }
    }
    return result;
}

std::vector<expression_pair> PropertyKeyValCollection::getKeyVals(
    std::shared_ptr<Expression> variable) const {
    std::vector<expression_pair> result;
    if (!propertyKeyValMap.contains(variable)) {
        return result;
    }
    for (auto& [_, keyVal] : propertyKeyValMap.at(variable)) {
        result.push_back(keyVal);
    }
    return result;
}

bool PropertyKeyValCollection::hasKeyVal(
    std::shared_ptr<Expression> variable, const std::string& propertyName) const {
    if (!propertyKeyValMap.contains(variable)) {
        return false;
    }
    if (!propertyKeyValMap.at(variable).contains(propertyName)) {
        return false;
    }
    return true;
}

expression_pair PropertyKeyValCollection::getKeyVal(
    std::shared_ptr<Expression> variable, const std::string& propertyName) const {
    KU_ASSERT(hasKeyVal(variable, propertyName));
    return propertyKeyValMap.at(variable).at(propertyName);
}

} // namespace binder
} // namespace kuzu
