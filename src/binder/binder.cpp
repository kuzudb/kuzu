#include "binder/binder.h"

namespace kuzu {
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
        return bindCopy(statement);
    }
    case StatementType::DROP_PROPERTY: {
        return bindDropProperty(statement);
    }
    case StatementType::ADD_PROPERTY: {
        return bindAddProperty(statement);
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

table_id_t Binder::bindRelTableID(const string& tableName) const {
    if (!catalog.getReadOnlyVersion()->containRelTable(tableName)) {
        throw BinderException("Rel table " + tableName + " does not exist.");
    }
    return catalog.getReadOnlyVersion()->getTableID(tableName);
}

table_id_t Binder::bindNodeTableID(const string& tableName) const {
    if (!catalog.getReadOnlyVersion()->containNodeTable(tableName)) {
        throw BinderException("Node table " + tableName + " does not exist.");
    }
    return catalog.getReadOnlyVersion()->getTableID(tableName);
}

shared_ptr<Expression> Binder::createVariable(const string& name, const DataType& dataType) {
    if (variablesInScope.contains(name)) {
        throw BinderException("Variable " + name + " already exists.");
    }
    auto uniqueName = getUniqueExpressionName(name);
    auto variable = make_shared<Expression>(ExpressionType::VARIABLE, dataType, uniqueName);
    variable->setRawName(name);
    variable->setAlias(name);
    variablesInScope.insert({name, variable});
    return variable;
}

void Binder::validateFirstMatchIsNotOptional(const SingleQuery& singleQuery) {
    if (singleQuery.isFirstReadingClauseOptionalMatch()) {
        throw BinderException("First match clause cannot be optional match.");
    }
}

void Binder::validateProjectionColumnNamesAreUnique(const expression_vector& expressions) {
    auto existColumnNames = unordered_set<string>();
    for (auto& expression : expressions) {
        auto columnName =
            expression->hasAlias() ? expression->getAlias() : expression->getRawName();
        if (existColumnNames.contains(columnName)) {
            throw BinderException(
                "Multiple result column with the same name " + columnName + " are not supported.");
        }
        existColumnNames.insert(columnName);
    }
}

void Binder::validateProjectionColumnHasNoInternalType(const expression_vector& expressions) {
    auto internalTypes = unordered_set<DataTypeID>{NODE_ID};
    for (auto& expression : expressions) {
        if (internalTypes.contains(expression->dataType.typeID)) {
            throw BinderException("Cannot return expression " + expression->getRawName() +
                                  " with internal type " +
                                  Types::dataTypeToString(expression->dataType));
        }
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
    auto expressionsToProject = boundSingleQueries[0]->getExpressionsToCollect();
    for (auto i = 1u; i < boundSingleQueries.size(); i++) {
        auto expressionsToProjectToCheck = boundSingleQueries[i]->getExpressionsToCollect();
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

void Binder::validateReturnNotFollowUpdate(const NormalizedSingleQuery& singleQuery) {
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        auto normalizedQueryPart = singleQuery.getQueryPart(i);
        if (normalizedQueryPart->hasUpdatingClause() && normalizedQueryPart->hasProjectionBody()) {
            throw BinderException("Return/With after update is not supported.");
        }
    }
}

void Binder::validateReadNotFollowUpdate(const NormalizedSingleQuery& singleQuery) {
    bool hasSeenUpdateClause = false;
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        auto normalizedQueryPart = singleQuery.getQueryPart(i);
        if (hasSeenUpdateClause && normalizedQueryPart->hasReadingClause()) {
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
    for (auto i = 0; i < size(CopyConfig::STRING_CSV_PARSING_OPTIONS); i++) {
        if (parsingOptionName == CopyConfig::STRING_CSV_PARSING_OPTIONS[i]) {
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
    variablesInScope = std::move(prevVariablesInScope);
}

} // namespace binder
} // namespace kuzu
