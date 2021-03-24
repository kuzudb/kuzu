#include "src/planner/include/binder.h"

namespace graphflow {
namespace planner {

static void validateQueryNodeWithSameName(QueryNode& queryNodeInGraph, QueryNode& queryNodeToMerge);

static void rebindSrcAndDstNode(QueryRel* queryRel, const QueryGraph& queryGraph);

void mergeQueryGraphs(QueryGraph& mergedQueryGraph, QueryGraph& otherQueryGraph);

unique_ptr<QueryGraph> Binder::bindSingleQuery(const SingleQuery& singleQuery) {
    auto mergedQueryGraph = make_unique<QueryGraph>();
    for (auto i = 0u; i < singleQuery.getNumStatements(); ++i) {
        mergeQueryGraphs(
            *mergedQueryGraph, bindStatement(singleQuery.getMatchStatement(i))->getQueryGraph());
    }
    return mergedQueryGraph;
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

label_t Binder::bindRelLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containRelLabel(parsed_label.c_str())) {
        throw invalid_argument("Rel label: " + parsed_label + " does not exist.");
    }
    return catalog.getRelLabelFromString(parsed_label.c_str());
}

label_t Binder::bindNodeLabel(const string& parsed_label) {
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

// TODO: move this function out of binder
void mergeQueryGraphs(QueryGraph& mergedQueryGraph, QueryGraph& otherQueryGraph) {
    for (auto& nameAndQueryNode : otherQueryGraph.nameToQueryNodeMap) {
        if (mergedQueryGraph.containsQueryNode(nameAndQueryNode.first)) {
            validateQueryNodeWithSameName(*mergedQueryGraph.getQueryNode(nameAndQueryNode.first),
                *otherQueryGraph.getQueryNode(nameAndQueryNode.first));
        } else {
            mergedQueryGraph.addQueryNode(move(nameAndQueryNode.second));
        }
    }
    for (auto& nameAndQueryRel : otherQueryGraph.nameToQueryRelMap) {
        if (mergedQueryGraph.containsQueryRel(nameAndQueryRel.first)) {
            throw invalid_argument("Reuse name: " + nameAndQueryRel.first + " for relationship.");
        } else {
            rebindSrcAndDstNode(nameAndQueryRel.second.get(), mergedQueryGraph);
            mergedQueryGraph.addQueryRel(move(nameAndQueryRel.second));
        }
    }
}

void validateQueryNodeWithSameName(QueryNode& queryNodeInGraph, QueryNode& queryNodeToMerge) {
    if (ANY_LABEL == queryNodeInGraph.getLabel() && ANY_LABEL != queryNodeToMerge.getLabel()) {
        queryNodeInGraph.setLabel(queryNodeToMerge.getLabel());
        return;
    }
    if (ANY_LABEL != queryNodeInGraph.getLabel() &&
        queryNodeToMerge.getLabel() != queryNodeToMerge.getLabel()) {
        throw invalid_argument("Multi-label query nodes are not supported. " +
                               queryNodeInGraph.getName() + " is given multiple labels.");
    }
}

void rebindSrcAndDstNode(QueryRel* queryRel, const QueryGraph& queryGraph) {
    queryRel->setSrcNode(queryGraph.getQueryNode(queryRel->getSrcNode()->getName()));
    queryRel->setDstNode(queryGraph.getQueryNode(queryRel->getDstNode()->getName()));
}

} // namespace planner
} // namespace graphflow
