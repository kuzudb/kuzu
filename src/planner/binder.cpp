#include "src/planner/include/binder.h"

namespace graphflow {
namespace planner {

vector<unique_ptr<BoundMatchStatement>> Binder::bindSingleQuery(const SingleQuery& singleQuery) {
    vector<unique_ptr<BoundMatchStatement>> boundStatements;
    for (auto i = 0; i < singleQuery.getNumStatements(); ++i) {
        boundStatements.push_back(bindStatement(singleQuery.getMatchStatement(i)));
    }
    return boundStatements;
}

unique_ptr<BoundMatchStatement> Binder::bindStatement(const MatchStatement& matchStatement) {
    auto queryGraph = make_unique<QueryGraph>();
    for (auto i = 0; i < matchStatement.getNumPatternElement(); ++i) {
        bindQueryRels(matchStatement.getPatternElement(i), *queryGraph);
    }
    if (!queryGraph->isConnected()) {
        throw invalid_argument("Disconnected query graph is not yet supported.");
    }
    return make_unique<BoundMatchStatement>(move(queryGraph));
}

void Binder::bindQueryRels(const PatternElement& patternElement, QueryGraph& queryGraph) {
    QueryNode* leftNode = bindQueryNode(patternElement.getNodePattern(), queryGraph);
    for (auto i = 0; i < patternElement.getNumPatternElementChain(); ++i) {
        auto rel = bindQueryRel(patternElement.getPatternElementChain(i), leftNode, queryGraph);
        leftNode = ArrowDirection::RIGHT ==
                           patternElement.getPatternElementChain(i).getRelPattern().getDirection() ?
                       rel->getDstNode() :
                       rel->getSrcNode();
    }
}

QueryRel* Binder::bindQueryRel(
    const PatternElementChain& patternElementChain, QueryNode* leftNode, QueryGraph& queryGraph) {
    auto parsedName = patternElementChain.getRelPattern().getName();
    if (parsedName.empty()) {
        parsedName = "_gfR_" + to_string(queryGraph.numQueryRels());
    } else if (queryGraph.containsQueryRel(parsedName)) {
        throw invalid_argument("Reuse name: " + parsedName + " for different relationships.");
    } else if (queryGraph.containsQueryNode(parsedName)) {
        throw invalid_argument("Reuse name: " + parsedName + " for node and relationship.");
    }
    auto queryRel = make_unique<QueryRel>(
        parsedName, bindRelLabel(patternElementChain.getRelPattern().getType()));
    auto leftNodeIsSrc =
        ArrowDirection::RIGHT == patternElementChain.getRelPattern().getDirection();
    bindNodeToRel(queryRel.get(), leftNode, leftNodeIsSrc);
    bindNodeToRel(queryRel.get(), bindQueryNode(patternElementChain.getNodePattern(), queryGraph),
        !leftNodeIsSrc);
    queryGraph.addQueryRel(move(queryRel));
    return queryGraph.getQueryRel(parsedName);
}

QueryNode* Binder::bindQueryNode(const NodePattern& nodePattern, QueryGraph& queryGraph) {
    auto parsedName = nodePattern.getName();
    if (parsedName.empty()) { // create anonymous node
        parsedName = "_gfN_" + to_string(queryGraph.numQueryNodes());
    } else if (queryGraph.containsQueryRel(parsedName)) {
        throw invalid_argument("Reuse name: " + parsedName + " for node and relationship");
    } else if (queryGraph.containsQueryNode(parsedName)) { // bind to previous bound node
        auto label = bindNodeLabel(nodePattern.getLabel());
        if (ANY_LABEL == label || queryGraph.getQueryNode(parsedName)->getLabel() == label) {
            return queryGraph.getQueryNode(parsedName);
        } else {
            throw invalid_argument("Multi-label query nodes are not supported. " + parsedName +
                                   " is given multiple labels");
        }
    }
    auto queryNode = make_unique<QueryNode>(parsedName, bindNodeLabel(nodePattern.getLabel()));
    queryGraph.addQueryNode(move(queryNode));
    return queryGraph.getQueryNode(parsedName);
}

int Binder::bindRelLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containRelLabel(parsed_label.c_str())) {
        throw invalid_argument("Rel label: " + parsed_label + " does not exist.");
    }
    return catalog.getRelLabelFromString(parsed_label.c_str());
}

int Binder::bindNodeLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containNodeLabel(parsed_label.c_str())) {
        throw invalid_argument("Node label: " + parsed_label + " does not exist.");
    }
    return catalog.getNodeLabelFromString(parsed_label.c_str());
}

void Binder::bindNodeToRel(QueryRel* queryRel, QueryNode* queryNode, bool isSrcNode) {
    if (ANY_LABEL != queryNode->getLabel() && ANY_LABEL != queryRel->getLabel()) {
        auto relLabels =
            catalog.getRelLabelsForNodeLabelDirection(queryNode->getLabel(), isSrcNode ? FWD : BWD);
        for (auto it = begin(relLabels); it != end(relLabels); ++it) {
            if (*it == queryRel->getLabel()) {
                isSrcNode ? queryRel->setSrcNode(queryNode) : queryRel->setDstNode(queryNode);
                return;
            }
        }
        throw invalid_argument(
            "Node: " + queryNode->getName() +
            " doesn't connect to edge with same type as: " + queryRel->getName());
    }
    isSrcNode ? queryRel->setSrcNode(queryNode) : queryRel->setDstNode(queryNode);
}

} // namespace planner
} // namespace graphflow
