#include "binder/binder.h"

#include "binder/bound_statement_rewriter.h"
#include "binder/expression/variable_expression.h"
#include "catalog/rel_table_schema.h"
#include "common/exception/binder.h"
#include "common/exception/not_implemented.h"
#include "common/string_utils.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bind(const Statement& statement) {
    std::unique_ptr<BoundStatement> boundStatement;
    switch (statement.getStatementType()) {
    case StatementType::CREATE_TABLE: {
        boundStatement = bindCreateTable(statement);
    } break;
    case StatementType::COPY_FROM: {
        boundStatement = bindCopyFromClause(statement);
    } break;
    case StatementType::COPY_TO: {
        boundStatement = bindCopyToClause(statement);
    } break;
    case StatementType::DROP_TABLE: {
        boundStatement = bindDropTable(statement);
    } break;
    case StatementType::RENAME_TABLE: {
        boundStatement = bindRenameTable(statement);
    } break;
    case StatementType::ADD_PROPERTY: {
        boundStatement = bindAddProperty(statement);
    } break;
    case StatementType::DROP_PROPERTY: {
        boundStatement = bindDropProperty(statement);
    } break;
    case StatementType::RENAME_PROPERTY: {
        boundStatement = bindRenameProperty(statement);
    } break;
    case StatementType::QUERY: {
        boundStatement = bindQuery((const RegularQuery&)statement);
    } break;
    case StatementType::STANDALONE_CALL: {
        boundStatement = bindStandaloneCall(statement);
    } break;
    case StatementType::COMMENT_ON: {
        boundStatement = bindCommentOn(statement);
    } break;
    case StatementType::EXPLAIN: {
        boundStatement = bindExplain(statement);
    } break;
    case StatementType::CREATE_MACRO: {
        boundStatement = bindCreateMacro(statement);
    } break;
    case StatementType::TRANSACTION: {
        boundStatement = bindTransaction(statement);
    } break;
    default:
        throw NotImplementedException("Binder::bind");
    }
    BoundStatementRewriter::rewrite(*boundStatement);
    return boundStatement;
}

std::shared_ptr<Expression> Binder::bindWhereExpression(const ParsedExpression& parsedExpression) {
    auto whereExpression = expressionBinder.bindExpression(parsedExpression);
    ExpressionBinder::implicitCastIfNecessary(whereExpression, LogicalTypeID::BOOL);
    return whereExpression;
}

common::table_id_t Binder::bindTableID(const std::string& tableName) const {
    auto catalogContent = catalog.getReadOnlyVersion();
    if (!catalogContent->containsTable(tableName)) {
        throw BinderException(
            common::StringUtils::string_format("Table {} does not exist.", tableName));
    }
    return catalogContent->getTableID(tableName);
}

std::shared_ptr<Expression> Binder::createVariable(
    const std::string& name, LogicalTypeID logicalTypeID) {
    return createVariable(name, LogicalType{logicalTypeID});
}

std::shared_ptr<Expression> Binder::createVariable(
    const std::string& name, const LogicalType& dataType) {
    if (scope->contains(name)) {
        throw BinderException("Variable " + name + " already exists.");
    }
    auto uniqueName = getUniqueExpressionName(name);
    auto expression = expressionBinder.createVariableExpression(dataType, uniqueName, name);
    expression->setAlias(name);
    scope->addExpression(name, expression);
    return expression;
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
    const std::vector<std::unique_ptr<NormalizedSingleQuery>>& normalizedSingleQueries) {
    if (normalizedSingleQueries.size() <= 1) {
        return;
    }
    auto columns = normalizedSingleQueries[0]->getStatementResult()->getColumns();
    for (auto i = 1u; i < normalizedSingleQueries.size(); i++) {
        auto otherColumns = normalizedSingleQueries[i]->getStatementResult()->getColumns();
        if (columns.size() != otherColumns.size()) {
            throw BinderException("The number of columns to union/union all must be the same.");
        }
        // Check whether the dataTypes in union expressions are exactly the same in each single
        // query.
        for (auto j = 0u; j < columns.size(); j++) {
            ExpressionBinder::validateExpectedDataType(
                *otherColumns[j], columns[j]->dataType.getLogicalTypeID());
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

void Binder::validateReadNotFollowUpdate(const NormalizedSingleQuery& singleQuery) {
    bool hasSeenUpdateClause = false;
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        auto normalizedQueryPart = singleQuery.getQueryPart(i);
        if (hasSeenUpdateClause && normalizedQueryPart->hasReadingClause()) {
            throw BinderException(
                "Read after update is not supported. Try query with multiple statements.");
        }
        hasSeenUpdateClause |= normalizedQueryPart->hasUpdatingClause();
    }
}

void Binder::validateTableType(table_id_t tableID, TableType expectedTableType) {
    if (catalog.getReadOnlyVersion()->getTableSchema(tableID)->tableType != expectedTableType) {
        throw BinderException("aa");
    }
}

void Binder::validateTableExist(const std::string& tableName) {
    if (!catalog.getReadOnlyVersion()->containsTable(tableName)) {
        throw BinderException("Table " + tableName + " does not exist.");
    }
}

void Binder::validateNodeRelTableExist(const std::string& tableName) {
    if (!catalog.getReadOnlyVersion()->containsNodeTable(tableName) &&
        !catalog.getReadOnlyVersion()->containsRelTable(tableName)) {
        throw BinderException("Table " + tableName + " does not exist.");
    }
}

void Binder::validateNodeTableHasNoEdge(const Catalog& _catalog, table_id_t tableID) {
    for (auto& tableSchema : _catalog.getReadOnlyVersion()->getRelTableSchemas()) {
        auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
        if (relTableSchema->isSrcOrDstTable(tableID)) {
            throw BinderException(StringUtils::string_format(
                "Cannot delete a node table with edges. It is on the edges of rel: {}.",
                relTableSchema->tableName));
        }
    }
}

std::string Binder::getUniqueExpressionName(const std::string& name) {
    return "_" + std::to_string(lastExpressionId++) + "_" + name;
}

std::unique_ptr<BinderScope> Binder::saveScope() {
    return scope->copy();
}

void Binder::restoreScope(std::unique_ptr<BinderScope> prevVariableScope) {
    scope = std::move(prevVariableScope);
}

} // namespace binder
} // namespace kuzu
