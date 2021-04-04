#include "src/planner/include/binder.h"

namespace graphflow {
namespace planner {

static void validateQueryNodeWithSameName(QueryNode& queryNodeInGraph, QueryNode& queryNodeToMerge);

static void rebindSrcAndDstNode(QueryRel* queryRel, const QueryGraph& queryGraph);

void mergeQueryGraphs(QueryGraph& mergedQueryGraph, QueryGraph& otherQueryGraph);

vector<unique_ptr<BoundMatchStatement>> Binder::bindSingleQuery(const SingleQuery& singleQuery) {
    vector<unique_ptr<BoundMatchStatement>> boundStatements;
    for (auto& statement : singleQuery.statements) {
        boundStatements.push_back(bindStatement(*statement));
    }
    return boundStatements;
}

unique_ptr<BoundMatchStatement> Binder::bindStatement(const MatchStatement& matchStatement) {
    auto queryGraph = make_unique<QueryGraph>();
    for (auto& patternElement : matchStatement.graphPattern) {
        bindQueryRels(*patternElement, *queryGraph);
    }
    if (!queryGraph->isConnected()) {
        throw invalid_argument("Disconnected query graph is not yet supported.");
    }
    if (matchStatement.whereClause) {
        auto expressionBinder = make_unique<ExpressionBinder>(*queryGraph, catalog);
        return make_unique<BoundMatchStatement>(
            move(queryGraph), expressionBinder->bindExpression(*matchStatement.whereClause));
    }
    return make_unique<BoundMatchStatement>(move(queryGraph));
}

void Binder::bindQueryRels(const PatternElement& patternElement, QueryGraph& queryGraph) {
    QueryNode* leftNode = bindQueryNode(*patternElement.nodePattern, queryGraph);
    for (auto& patternElementChain : patternElement.patternElementChains) {
        auto rel = bindQueryRel(*patternElementChain, leftNode, queryGraph);
        leftNode = ArrowDirection::RIGHT == patternElementChain->relPattern->arrowDirection ?
                       rel->getDstNode() :
                       rel->getSrcNode();
    }
}

QueryRel* Binder::bindQueryRel(
    const PatternElementChain& patternElementChain, QueryNode* leftNode, QueryGraph& queryGraph) {
    auto parsedName = patternElementChain.relPattern->name;
    if (parsedName.empty()) {
        parsedName = "_gfR_" + to_string(queryGraph.numQueryRels());
    } else if (queryGraph.containsQueryRel(parsedName)) {
        throw invalid_argument("Reuse name: " + parsedName + " for different relationships.");
    } else if (queryGraph.containsQueryNode(parsedName)) {
        throw invalid_argument("Reuse name: " + parsedName + " for node and relationship.");
    }
    auto queryRel =
        make_unique<QueryRel>(parsedName, bindRelLabel(patternElementChain.relPattern->label));
    auto leftNodeIsSrc = ArrowDirection::RIGHT == patternElementChain.relPattern->arrowDirection;
    bindNodeToRel(queryRel.get(), leftNode, leftNodeIsSrc);
    bindNodeToRel(queryRel.get(), bindQueryNode(*patternElementChain.nodePattern, queryGraph),
        !leftNodeIsSrc);
    queryGraph.addQueryRel(move(queryRel));
    return queryGraph.getQueryRel(parsedName);
}

QueryNode* Binder::bindQueryNode(const NodePattern& nodePattern, QueryGraph& queryGraph) {
    auto parsedName = nodePattern.name;
    if (parsedName.empty()) { // create anonymous node
        parsedName = "_gfN_" + to_string(queryGraph.numQueryNodes());
    } else if (queryGraph.containsQueryRel(parsedName)) {
        throw invalid_argument("Reuse name: " + parsedName + " for node and relationship");
    } else if (queryGraph.containsQueryNode(parsedName)) { // bind to previous bound node
        auto label = bindNodeLabel(nodePattern.label);
        if (ANY_LABEL == label || queryGraph.getQueryNode(parsedName)->getLabel() == label) {
            return queryGraph.getQueryNode(parsedName);
        } else {
            throw invalid_argument("Multi-label query nodes are not supported. " + parsedName +
                                   " is given multiple labels");
        }
    }
    auto queryNode = make_unique<QueryNode>(parsedName, bindNodeLabel(nodePattern.label));
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
        for (auto& relLabel : relLabels) {
            if (relLabel == queryRel->getLabel()) {
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
