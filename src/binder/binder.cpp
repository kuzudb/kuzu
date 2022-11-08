#include "src/binder/include/binder.h"

namespace graphflow {
namespace binder {

unique_ptr<BoundStatement> Binder::bind(const Statement& statement) {
    switch (statement.getStatementType()) {
    case StatementType::CREATE_NODE_CLAUSE: {
        return bindCreateNodeClause(statement);
    }
    case StatementType::CREATE_REL_CLAUSE: {
        return bindCreateRelClause(statement);
    }
    case StatementType::DROP_TABLE: {
        return bindDropTable(statement);
    }
    case StatementType::COPY_CSV: {
        return bindCopyCSV(statement);
    }
    case StatementType::QUERY: {
        return bindQuery((const RegularQuery&)statement);
    }
    default:
        assert(false);
    }
}

shared_ptr<Expression> Binder::bindWhereExpression(const ParsedExpression& parsedExpression) {
    auto whereExpression = expressionBinder.bindExpression(parsedExpression);
    ExpressionBinder::implicitCastIfNecessary(whereExpression, BOOL);
    return whereExpression;
}

table_id_t Binder::bindRelTable(const string& tableName) const {
    if (tableName.empty()) {
        return ANY_TABLE_ID;
    }
    if (!catalog.getReadOnlyVersion()->containRelTable(tableName)) {
        throw BinderException("Rel table " + tableName + " does not exist.");
    }
    return catalog.getReadOnlyVersion()->getRelTableIDFromName(tableName);
}

table_id_t Binder::bindNodeTable(const string& tableName) const {
    if (tableName.empty()) {
        return ANY_TABLE_ID;
    }
    if (!catalog.getReadOnlyVersion()->containNodeTable(tableName)) {
        throw BinderException("Node table " + tableName + " does not exist.");
    }
    return catalog.getReadOnlyVersion()->getNodeTableIDFromName(tableName);
}

void Binder::validateFirstMatchIsNotOptional(const SingleQuery& singleQuery) {
    if (singleQuery.isFirstReadingClauseOptionalMatch()) {
        throw BinderException("First match clause cannot be optional match.");
    }
}

void Binder::validateNodeAndRelTableIsConnected(const Catalog& catalog_, table_id_t relTableID,
    table_id_t nodeTableID, RelDirection direction) {
    assert(relTableID != ANY_TABLE_ID);
    assert(nodeTableID != ANY_TABLE_ID);
    auto connectedRelTableIDs =
        catalog_.getReadOnlyVersion()->getRelTableIDsForNodeTableDirection(nodeTableID, direction);
    for (auto& connectedRelTableID : connectedRelTableIDs) {
        if (relTableID == connectedRelTableID) {
            return;
        }
    }
    throw BinderException("Node table " +
                          catalog_.getReadOnlyVersion()->getNodeTableName(nodeTableID) +
                          " doesn't connect to rel table " +
                          catalog_.getReadOnlyVersion()->getRelTableName(relTableID) + ".");
}

void Binder::validateProjectionColumnNamesAreUnique(const expression_vector& expressions) {
    auto existColumnNames = unordered_set<string>();
    for (auto& expression : expressions) {
        if (existColumnNames.contains(expression->getRawName())) {
            throw BinderException("Multiple result column with the same name " +
                                  expression->getRawName() + " are not supported.");
        }
        existColumnNames.insert(expression->getRawName());
    }
}

void Binder::validateProjectionColumnsInWithClauseAreAliased(const expression_vector& expressions) {
    for (auto& expression : expressions) {
        if (!expression->hasAlias()) {
            throw BinderException("Expression in WITH must be aliased (use AS).");
        }
    }
}

void Binder::validateOrderByFollowedBySkipOrLimitInWithClause(
    const BoundProjectionBody& boundProjectionBody) {
    auto hasSkipOrLimit = boundProjectionBody.hasSkip() || boundProjectionBody.hasLimit();
    if (boundProjectionBody.hasOrderByExpressions() && !hasSkipOrLimit) {
        throw BinderException("In WITH clause, ORDER BY must be followed by SKIP or LIMIT.");
    }
}

void Binder::validateUnionColumnsOfTheSameType(
    const vector<unique_ptr<BoundSingleQuery>>& boundSingleQueries) {
    if (boundSingleQueries.size() <= 1) {
        return;
    }
    auto expressionsToProject = boundSingleQueries[0]->getExpressionsToReturn();
    for (auto i = 1u; i < boundSingleQueries.size(); i++) {
        auto expressionsToProjectToCheck = boundSingleQueries[i]->getExpressionsToReturn();
        if (expressionsToProject.size() != expressionsToProjectToCheck.size()) {
            throw BinderException("The number of columns to union/union all must be the same.");
        }
        // Check whether the dataTypes in union expressions are exactly the same in each single
        // query.
        for (auto j = 0u; j < expressionsToProject.size(); j++) {
            ExpressionBinder::validateExpectedDataType(
                *expressionsToProjectToCheck[j], expressionsToProject[j]->dataType.typeID);
        }
    }
}

void Binder::validateIsAllUnionOrUnionAll(const BoundRegularQuery& regularQuery) {
    auto unionAllExpressionCounter = 0u;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries() - 1; i++) {
        unionAllExpressionCounter += regularQuery.getIsUnionAll(i);
    }
    if ((0 < unionAllExpressionCounter) &&
        (unionAllExpressionCounter < regularQuery.getNumSingleQueries() - 1)) {
        throw BinderException("Union and union all can't be used together.");
    }
}

void Binder::validateReadNotFollowUpdate(const NormalizedSingleQuery& normalizedSingleQuery) {
    bool hasSeenUpdateClause = false;
    for (auto i = 0u; i < normalizedSingleQuery.getNumQueryParts(); ++i) {
        auto normalizedQueryPart = normalizedSingleQuery.getQueryPart(i);
        if (hasSeenUpdateClause && normalizedQueryPart->getNumReadingClause() != 0) {
            throw BinderException("Read after update is not supported.");
        }
        hasSeenUpdateClause |= normalizedQueryPart->hasUpdatingClause();
    }
}

void Binder::validateTableExist(const Catalog& _catalog, string& tableName) {
    if (!_catalog.getReadOnlyVersion()->containNodeTable(tableName) &&
        !_catalog.getReadOnlyVersion()->containRelTable(tableName)) {
        throw BinderException("Node/Rel " + tableName + " does not exist.");
    }
}

bool Binder::validateStringParsingOptionName(string& parsingOptionName) {
    for (auto i = 0; i < size(CopyCSVConfig::STRING_CSV_PARSING_OPTIONS); i++) {
        if (parsingOptionName == CopyCSVConfig::STRING_CSV_PARSING_OPTIONS[i]) {
            return true;
        }
    }
    return false;
}

void Binder::validateNodeTableHasNoEdge(const Catalog& _catalog, table_id_t tableID) {
    for (auto& tableIDSchema : _catalog.getReadOnlyVersion()->getRelTableSchemas()) {
        if (tableIDSchema.second->edgeContainsNodeTable(tableID)) {
            throw BinderException(StringUtils::string_format(
                "Cannot delete a node table with edges. It is on the edges of rel: %s.",
                tableIDSchema.second->tableName.c_str()));
        }
    }
}

string Binder::getUniqueExpressionName(const string& name) {
    return "_" + to_string(lastExpressionId++) + "_" + name;
}

unordered_map<string, shared_ptr<Expression>> Binder::enterSubquery() {
    return variablesInScope;
}

void Binder::exitSubquery(unordered_map<string, shared_ptr<Expression>> prevVariablesInScope) {
    variablesInScope = move(prevVariablesInScope);
}

} // namespace binder
} // namespace graphflow
