#include "binder/binder.h"

#include "binder/expression/variable_expression.h"
#include "common/string_utils.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bind(const Statement& statement) {
    switch (statement.getStatementType()) {
    case StatementType::CREATE_NODE_TABLE: {
        return bindCreateNodeTableClause(statement);
    }
    case StatementType::CREATE_REL_TABLE: {
        return bindCreateRelTableClause(statement);
    }
    case StatementType::COPY: {
        return bindCopyClause(statement);
    }
    case StatementType::DROP_TABLE: {
        return bindDropTableClause(statement);
    }
    case StatementType::RENAME_TABLE: {
        return bindRenameTableClause(statement);
    }
    case StatementType::ADD_PROPERTY: {
        return bindAddPropertyClause(statement);
    }
    case StatementType::DROP_PROPERTY: {
        return bindDropPropertyClause(statement);
    }
    case StatementType::RENAME_PROPERTY: {
        return bindRenamePropertyClause(statement);
    }
    case StatementType::QUERY: {
        return bindQuery((const RegularQuery&)statement);
    }
    case StatementType::CALL: {
        return bindCallClause(statement);
    }
    default:
        assert(false);
    }
}

std::shared_ptr<Expression> Binder::bindWhereExpression(const ParsedExpression& parsedExpression) {
    auto whereExpression = expressionBinder.bindExpression(parsedExpression);
    ExpressionBinder::implicitCastIfNecessary(whereExpression, LogicalTypeID::BOOL);
    return whereExpression;
}

table_id_t Binder::bindRelTableID(const std::string& tableName) const {
    if (!catalog.getReadOnlyVersion()->containRelTable(tableName)) {
        throw BinderException("Rel table " + tableName + " does not exist.");
    }
    return catalog.getReadOnlyVersion()->getTableID(tableName);
}

table_id_t Binder::bindNodeTableID(const std::string& tableName) const {
    if (!catalog.getReadOnlyVersion()->containNodeTable(tableName)) {
        throw BinderException("Node table " + tableName + " does not exist.");
    }
    return catalog.getReadOnlyVersion()->getTableID(tableName);
}

std::shared_ptr<Expression> Binder::createVariable(
    const std::string& name, const LogicalType& dataType) {
    if (variableScope->contains(name)) {
        throw BinderException("Variable " + name + " already exists.");
    }
    auto uniqueName = getUniqueExpressionName(name);
    auto expression = expressionBinder.createVariableExpression(dataType, uniqueName, name);
    expression->setAlias(name);
    variableScope->addExpression(name, expression);
    return expression;
}

void Binder::validateFirstMatchIsNotOptional(const SingleQuery& singleQuery) {
    if (singleQuery.isFirstReadingClauseOptionalMatch()) {
        throw BinderException("First match clause cannot be optional match.");
    }
}

void Binder::validateProjectionColumnNamesAreUnique(const expression_vector& expressions) {
    auto existColumnNames = std::unordered_set<std::string>();
    for (auto& expression : expressions) {
        auto columnName = expression->hasAlias() ? expression->getAlias() : expression->toString();
        if (existColumnNames.contains(columnName)) {
            throw BinderException(
                "Multiple result column with the same name " + columnName + " are not supported.");
        }
        existColumnNames.insert(columnName);
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
    const std::vector<std::unique_ptr<BoundSingleQuery>>& boundSingleQueries) {
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
            ExpressionBinder::validateExpectedDataType(*expressionsToProjectToCheck[j],
                expressionsToProject[j]->dataType.getLogicalTypeID());
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

void Binder::validateTableExist(const Catalog& _catalog, std::string& tableName) {
    if (!_catalog.getReadOnlyVersion()->containNodeTable(tableName) &&
        !_catalog.getReadOnlyVersion()->containRelTable(tableName)) {
        throw BinderException("Node/Rel " + tableName + " does not exist.");
    }
}

bool Binder::validateStringParsingOptionName(std::string& parsingOptionName) {
    for (auto i = 0; i < std::size(CopyConstants::STRING_CSV_PARSING_OPTIONS); i++) {
        if (parsingOptionName == CopyConstants::STRING_CSV_PARSING_OPTIONS[i]) {
            return true;
        }
    }
    return false;
}

void Binder::validateNodeTableHasNoEdge(const Catalog& _catalog, table_id_t tableID) {
    for (auto& tableIDSchema : _catalog.getReadOnlyVersion()->getRelTableSchemas()) {
        if (tableIDSchema.second->isSrcOrDstTable(tableID)) {
            throw BinderException(StringUtils::string_format(
                "Cannot delete a node table with edges. It is on the edges of rel: {}.",
                tableIDSchema.second->tableName));
        }
    }
}

std::string Binder::getUniqueExpressionName(const std::string& name) {
    return "_" + std::to_string(lastExpressionId++) + "_" + name;
}

std::unique_ptr<VariableScope> Binder::enterSubquery() {
    return variableScope->copy();
}

void Binder::exitSubquery(std::unique_ptr<VariableScope> prevVariableScope) {
    variableScope = std::move(prevVariableScope);
}

} // namespace binder
} // namespace kuzu
