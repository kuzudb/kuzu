#include "src/binder/include/query_binder.h"

#include <fstream>

#include "src/binder/include/bound_statements/bound_load_csv_statement.h"
#include "src/binder/include/bound_statements/bound_match_statement.h"
#include "src/binder/include/expression/literal_expression.h"
#include "src/binder/include/expression_binder.h"
#include "src/common/include/csv_reader/csv_reader.h"

namespace graphflow {
namespace binder {

const char DEFAULT_LOAD_CSV_TOKEN_SEPARATOR = ',';

static string makeUniqueVariableName(const string& name, uint32_t& idx);
static vector<pair<string, DataType>> parseCSVHeader(const string& headerLine, char tokenSeparator);
static vector<unique_ptr<BoundReadingStatement>> mergeAllMatchStatements(
    vector<unique_ptr<BoundReadingStatement>> boundReadingStatements);

static void validateCSVHeaderColumnNamesAreUnique(const vector<pair<string, DataType>>& headerInfo);
static void validateProjectionColumnNamesAreUnique(
    const vector<shared_ptr<Expression>>& expressions);
static void validateOnlyFunctionIsCountStar(vector<shared_ptr<Expression>>& expressions);
static void validateQueryGraphIsConnected(const QueryGraph& queryGraph,
    const unordered_map<string, shared_ptr<Expression>>& variablesInScope);

unique_ptr<BoundSingleQuery> QueryBinder::bind(const SingleQuery& singleQuery) {
    auto boundSingleQuery = bindSingleQuery(singleQuery);
    optimizeReadingStatements(*boundSingleQuery);
    return boundSingleQuery;
}

void QueryBinder::optimizeReadingStatements(BoundSingleQuery& boundSingleQuery) {
    for (auto& boundQueryPart : boundSingleQuery.boundQueryParts) {
        boundQueryPart->boundReadingStatements =
            mergeAllMatchStatements(move(boundQueryPart->boundReadingStatements));
    }
    boundSingleQuery.boundReadingStatements =
        mergeAllMatchStatements(move(boundSingleQuery.boundReadingStatements));
}

unique_ptr<BoundSingleQuery> QueryBinder::bindSingleQuery(const SingleQuery& singleQuery) {
    auto boundSingleQuery = make_unique<BoundSingleQuery>();
    for (auto& queryPart : singleQuery.queryParts) {
        boundSingleQuery->boundQueryParts.push_back(bindQueryPart(*queryPart));
    }
    for (auto& readingStatement : singleQuery.readingStatements) {
        boundSingleQuery->boundReadingStatements.push_back(bindReadingStatement(*readingStatement));
    }
    boundSingleQuery->boundReturnStatement = bindReturnStatement(*singleQuery.returnStatement);
    return boundSingleQuery;
}

unique_ptr<BoundQueryPart> QueryBinder::bindQueryPart(const QueryPart& queryPart) {
    auto boundQueryPart = make_unique<BoundQueryPart>();
    for (auto& readingStatement : queryPart.readingStatements) {
        boundQueryPart->boundReadingStatements.push_back(bindReadingStatement(*readingStatement));
    }
    boundQueryPart->boundWithStatement = bindWithStatement(*queryPart.withStatement);
    return boundQueryPart;
}

unique_ptr<BoundReadingStatement> QueryBinder::bindReadingStatement(
    const ReadingStatement& readingStatement) {
    if (MATCH_STATEMENT == readingStatement.statementType) {
        return bindMatchStatement((MatchStatement&)readingStatement);
    }
    return bindLoadCSVStatement((LoadCSVStatement&)readingStatement);
}

unique_ptr<BoundReadingStatement> QueryBinder::bindLoadCSVStatement(
    const LoadCSVStatement& loadCSVStatement) {
    auto tokenSeparator = DEFAULT_LOAD_CSV_TOKEN_SEPARATOR;
    if (!loadCSVStatement.fieldTerminator.empty()) {
        if (loadCSVStatement.fieldTerminator.size() > 1) {
            throw invalid_argument("FIELDTERMINATOR has data type STRING. CHAR was expected.");
        }
        tokenSeparator = loadCSVStatement.fieldTerminator.at(0);
    }
    auto boundInputExpression = ExpressionBinder(variablesInScope, catalog)
                                    .bindExpression(*loadCSVStatement.inputExpression);
    if (LITERAL_STRING != boundInputExpression->expressionType) {
        throw invalid_argument("LOAD CSV FROM " +
                               expressionTypeToString(boundInputExpression->expressionType) +
                               " is not supported. STRING was expected.");
    }
    auto filePath = static_pointer_cast<LiteralExpression>(boundInputExpression)->literal.strVal;
    auto fileStream = ifstream(filePath, ios_base::in);
    if (!fileStream.is_open()) {
        throw invalid_argument("Cannot open file at " + filePath + ".");
    }
    string headerLine;
    getline(fileStream, headerLine);
    auto headerInfo = parseCSVHeader(headerLine, tokenSeparator);
    validateCSVHeaderColumnNamesAreUnique(headerInfo);
    fileStream.seekg(0, ios_base::end);
    /**
     * In order to avoid creating LIST datatype or expression, we directly create
     * csvColumnExpression with type CSV_LINE_EXTRACT for each column e.g. csvLine[0], csvLine[1]
     * ... One consequence is we won't allow user to refer csvLine later. Instead, we force user to
     * write csvLine[i]. By doing so, we don't need to implement evaluator for list.
     */
    auto csvVariableAliasName = loadCSVStatement.lineVariableName;
    auto csvVariableName = makeUniqueVariableName(csvVariableAliasName, lastVariableIdx);
    auto csvColumnVariables = vector<shared_ptr<Expression>>();
    for (auto i = 0u; i < headerInfo.size(); ++i) {
        auto csvColumnVariableAliasName = csvVariableAliasName + "[" + to_string(i) + "]";
        auto csvColumnVariableName = csvVariableName + "[" + to_string(i) + "]";
        auto csvColumnVariable =
            make_shared<Expression>(CSV_LINE_EXTRACT, headerInfo[i].second, csvColumnVariableName);
        csvColumnVariable->alias = csvColumnVariableAliasName;
        variablesInScope.insert({csvColumnVariable->alias, csvColumnVariable});
        csvColumnVariables.push_back(csvColumnVariable);
    }
    return make_unique<BoundLoadCSVStatement>(filePath, tokenSeparator, move(csvColumnVariables));
}

unique_ptr<BoundReadingStatement> QueryBinder::bindMatchStatement(
    const MatchStatement& matchStatement) {
    auto prevVariablesInScope = variablesInScope;
    auto queryGraph = bindQueryGraph(matchStatement.graphPattern);
    validateQueryGraphIsConnected(*queryGraph, prevVariablesInScope);
    auto boundMatchStatement = make_unique<BoundMatchStatement>(move(queryGraph));
    if (matchStatement.whereClause) {
        boundMatchStatement->whereExpression = bindWhereExpression(*matchStatement.whereClause);
    }
    return boundMatchStatement;
}

unique_ptr<BoundWithStatement> QueryBinder::bindWithStatement(const WithStatement& withStatement) {
    auto expressionsToProject =
        bindProjectExpressions(withStatement.expressions, withStatement.containsStar);
    variablesInScope.clear();
    for (auto& expression : expressionsToProject) {
        if (expression->alias.empty()) {
            throw invalid_argument("Expression in WITH multi be aliased (use AS).");
        }
        variablesInScope.insert({expression->getAliasElseRawExpression(), expression});
    }
    auto boundWithStatement = make_unique<BoundWithStatement>(move(expressionsToProject));
    if (withStatement.whereClause) {
        boundWithStatement->whereExpression = bindWhereExpression(*withStatement.whereClause);
    }
    return boundWithStatement;
}

unique_ptr<BoundReturnStatement> QueryBinder::bindReturnStatement(
    const ReturnStatement& returnStatement) {
    return make_unique<BoundReturnStatement>(
        bindProjectExpressions(returnStatement.expressions, returnStatement.containsStar));
}

shared_ptr<Expression> QueryBinder::bindWhereExpression(const ParsedExpression& parsedExpression) {
    auto expressionBinder = make_unique<ExpressionBinder>(variablesInScope, catalog);
    auto whereExpression = expressionBinder->bindExpression(parsedExpression);
    if (BOOL != whereExpression->dataType) {
        throw invalid_argument("Type mismatch: " + whereExpression->rawExpression + " returns " +
                               TypeUtils::dataTypeToString(whereExpression->dataType) +
                               " expected Boolean.");
    }
    return whereExpression;
}

vector<shared_ptr<Expression>> QueryBinder::bindProjectExpressions(
    const vector<unique_ptr<ParsedExpression>>& parsedExpressions, bool projectStar) {
    auto expressionBinder = make_unique<ExpressionBinder>(variablesInScope, catalog);
    auto expressions = vector<shared_ptr<Expression>>();
    if (projectStar) {
        if (variablesInScope.empty()) {
            throw invalid_argument(
                "RETURN or WITH * is not allowed when there are no variables in scope.");
        }
        for (auto& [name, expression] : variablesInScope) {
            expressions.push_back(expression);
        }
    }
    for (auto& parsedExpression : parsedExpressions) {
        expressions.push_back(expressionBinder->bindExpression(*parsedExpression));
    }
    validateProjectionColumnNamesAreUnique(expressions);
    validateOnlyFunctionIsCountStar(expressions);
    return expressions;
}

unique_ptr<QueryGraph> QueryBinder::bindQueryGraph(
    const vector<unique_ptr<PatternElement>>& graphPattern) {
    auto queryGraph = make_unique<QueryGraph>();
    for (auto& patternElement : graphPattern) {
        auto leftNode = bindQueryNode(*patternElement->nodePattern, *queryGraph).get();
        for (auto& patternElementChain : patternElement->patternElementChains) {
            auto rightNode = bindQueryNode(*patternElementChain->nodePattern, *queryGraph).get();
            bindQueryRel(*patternElementChain->relPattern, leftNode, rightNode, *queryGraph);
            leftNode = rightNode;
        }
    }
    return queryGraph;
}

void QueryBinder::bindQueryRel(const RelPattern& relPattern, NodeExpression* leftNode,
    NodeExpression* rightNode, QueryGraph& queryGraph) {
    auto parsedName = relPattern.name;
    if (variablesInScope.contains(parsedName)) {
        auto variableInScope = variablesInScope.at(parsedName);
        if (REL != variableInScope->dataType) {
            throw invalid_argument(parsedName + " defined with conflicting type " +
                                   TypeUtils::dataTypeToString(variableInScope->dataType) +
                                   " (expect RELATIONSHIP).");
        } else {
            // Bind to queryRel in scope requires QueryRel takes multiple src & dst nodes
            // Example MATCH (a)-[r1]->(b) MATCH (c)-[r1]->(d)
            // Should be bound as (a,c)-[r1]->(b,d)
            throw invalid_argument("Bind relationship " + parsedName +
                                   " to relationship with same name is not supported.");
        }
    }
    auto queryRel = make_shared<RelExpression>(makeUniqueVariableName(parsedName, lastVariableIdx),
        bindRelLabel(relPattern.label), relPattern.lowerBound, relPattern.upperBound);
    queryRel->alias = parsedName;
    auto isLeftNodeSrc = RIGHT == relPattern.arrowDirection;
    bindNodeToRel(*queryRel, leftNode, isLeftNodeSrc);
    bindNodeToRel(*queryRel, rightNode, !isLeftNodeSrc);
    if (!queryRel->alias.empty()) {
        variablesInScope.insert({queryRel->alias, queryRel});
    }
    queryGraph.addQueryRelIfNotExist(queryRel);
}

void QueryBinder::bindNodeToRel(
    RelExpression& queryRel, NodeExpression* queryNode, bool isSrcNode) {
    if (ANY_LABEL != queryNode->label && ANY_LABEL != queryRel.label) {
        auto relLabels =
            catalog.getRelLabelsForNodeLabelDirection(queryNode->label, isSrcNode ? FWD : BWD);
        for (auto& relLabel : relLabels) {
            if (relLabel == queryRel.label) {
                isSrcNode ? queryRel.srcNode = queryNode : queryRel.dstNode = queryNode;
                return;
            }
        }
        throw invalid_argument("Node " + queryNode->getAliasElseRawExpression() +
                               " doesn't connect to edge with same type as " +
                               queryRel.getAliasElseRawExpression() + ".");
    }
    isSrcNode ? queryRel.srcNode = queryNode : queryRel.dstNode = queryNode;
}

shared_ptr<NodeExpression> QueryBinder::bindQueryNode(
    const NodePattern& nodePattern, QueryGraph& queryGraph) {
    auto parsedName = nodePattern.name;
    shared_ptr<NodeExpression> queryNode;
    if (variablesInScope.contains(parsedName)) { // bind to node in scope
        auto variableInScope = variablesInScope.at(parsedName);
        if (NODE != variableInScope->dataType) {
            throw invalid_argument(parsedName + " defined with conflicting type " +
                                   TypeUtils::dataTypeToString(variableInScope->dataType) +
                                   " (expect NODE).");
        }
        queryNode = static_pointer_cast<NodeExpression>(variableInScope);
        auto otherLabel = bindNodeLabel(nodePattern.label);
        if (ANY_LABEL == queryNode->label) {
            queryNode->label = otherLabel;
        } else if (ANY_LABEL != otherLabel && queryNode->label != otherLabel) {
            throw invalid_argument(
                "Multi-label is not supported. Node " + parsedName + " is given multiple labels.");
        }
    } else {
        queryNode = make_shared<NodeExpression>(
            makeUniqueVariableName(parsedName, lastVariableIdx), bindNodeLabel(nodePattern.label));
        queryNode->alias = parsedName;
        if (!queryNode->alias.empty()) {
            variablesInScope.insert({queryNode->alias, queryNode});
        }
    }
    queryGraph.addQueryNodeIfNotExist(queryNode);
    return queryNode;
}

label_t QueryBinder::bindRelLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containRelLabel(parsed_label)) {
        throw invalid_argument("Rel label " + parsed_label + " does not exist.");
    }
    return catalog.getRelLabelFromString(parsed_label.c_str());
}

label_t QueryBinder::bindNodeLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containNodeLabel(parsed_label)) {
        throw invalid_argument("Node label " + parsed_label + " does not exist.");
    }
    return catalog.getNodeLabelFromString(parsed_label.c_str());
}

string makeUniqueVariableName(const string& name, uint32_t& idx) {
    return "_gf" + to_string(idx++) + "_" + name;
}

vector<pair<string, DataType>> parseCSVHeader(const string& headerLine, char tokenSeparator) {
    auto headerInfo = vector<pair<string, DataType>>();
    for (auto& entry : StringUtils::split(headerLine, string(1, tokenSeparator))) {
        auto propertyDataType = StringUtils::split(entry, PROPERTY_DATATYPE_SEPARATOR);
        if (propertyDataType.size() == 1) {
            throw invalid_argument("Missing colon separated dataType in " + entry + ".");
        } else if (propertyDataType.size() > 2) {
            throw invalid_argument("Multiple dataType in " + entry + " is not supported.");
        }
        headerInfo.emplace_back(
            make_pair(propertyDataType[0], TypeUtils::getDataType(propertyDataType[1])));
    }
    return headerInfo;
}

// Merge all match statements as one given a list of reading statement.
// This is still valid in the case of MATCH LOAD CSV MATCH. Because our LOAD CSV reads header during
// binding, it cannot depend on runtime output of previous match. So the order of MATCH and LOAD CSV
// doesn't matter (similar to Cartesian Product).
vector<unique_ptr<BoundReadingStatement>> mergeAllMatchStatements(
    vector<unique_ptr<BoundReadingStatement>> boundReadingStatements) {
    auto result = vector<unique_ptr<BoundReadingStatement>>();
    auto mergedMatchStatement = make_unique<BoundMatchStatement>(make_unique<QueryGraph>());
    for (auto& boundReadingStatement : boundReadingStatements) {
        if (LOAD_CSV_STATEMENT == boundReadingStatement->statementType) {
            result.push_back(move(boundReadingStatement));
        } else {
            mergedMatchStatement->merge((BoundMatchStatement&)*boundReadingStatement);
        }
    }
    result.push_back(move(mergedMatchStatement));
    return result;
}

void validateCSVHeaderColumnNamesAreUnique(const vector<pair<string, DataType>>& headerInfo) {
    auto propertyNames = unordered_set<string>();
    for (auto& [propertyName, dataType] : headerInfo) {
        if (propertyNames.contains(propertyName)) {
            throw invalid_argument("Multiple property columns with the same name " + propertyName +
                                   " is not supported.");
        }
        propertyNames.insert(propertyName);
    }
}

void validateProjectionColumnNamesAreUnique(const vector<shared_ptr<Expression>>& expressions) {
    auto existColumnNames = unordered_set<string>();
    for (auto& expression : expressions) {
        auto k = *expression;
        if (end(existColumnNames) !=
            existColumnNames.find(expression->getAliasElseRawExpression())) {
            throw invalid_argument("Multiple result column with the same name " +
                                   expression->getAliasElseRawExpression() + " are not supported.");
        }
        existColumnNames.insert(expression->getAliasElseRawExpression());
    }
}

void validateOnlyFunctionIsCountStar(vector<shared_ptr<Expression>>& expressions) {
    for (auto& expression : expressions) {
        if (FUNCTION == expression->expressionType &&
            FUNCTION_COUNT_STAR == expression->variableName && expressions.size() != 1) {
            throw invalid_argument("The only function in the return clause should be COUNT(*).");
        }
    }
}

void validateQueryGraphIsConnected(const QueryGraph& queryGraph,
    const unordered_map<string, shared_ptr<Expression>>& variablesInScope) {
    auto visited = unordered_set<string>();
    for (auto& [name, variable] : variablesInScope) {
        if (NODE == variable->dataType) {
            visited.insert(variable->variableName);
        }
    }
    if (visited.empty()) {
        visited.insert(queryGraph.queryNodes[0]->variableName);
    }
    auto target = visited;
    for (auto& queryNode : queryGraph.queryNodes) {
        target.insert(queryNode->variableName);
    }
    auto frontier = visited;
    while (!frontier.empty()) {
        auto nextFrontier = unordered_set<string>();
        for (auto& nodeInFrontier : frontier) {
            auto nbrs = queryGraph.getNeighbourNodeNames(nodeInFrontier);
            for (auto& nbr : nbrs) {
                if (end(visited) == visited.find(nbr)) {
                    visited.insert(nbr);
                    nextFrontier.insert(nbr);
                }
            }
        }
        if (visited.size() == target.size()) {
            return;
        }
        frontier = nextFrontier;
    }
    throw invalid_argument("Disconnect query graph is not supported.");
}

} // namespace binder
} // namespace graphflow
