#include "binder/binder.h"

#include "binder/bound_statement_rewriter.h"
#include "common/copier_config/csv_reader_config.h"
#include "common/exception/binder.h"
#include "common/keyword/rdf_keyword.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "function/table_functions.h"
#include "main/client_context.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::parser;

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
    case StatementType::ALTER: {
        boundStatement = bindAlter(statement);
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
    case StatementType::EXTENSION: {
        boundStatement = bindExtension(statement);
    } break;
    case StatementType::EXPORT_DATABASE: {
        boundStatement = bindExportDatabaseClause(statement);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    BoundStatementRewriter::rewrite(*boundStatement, catalog);
    return boundStatement;
}

std::shared_ptr<Expression> Binder::bindWhereExpression(const ParsedExpression& parsedExpression) {
    auto whereExpression = expressionBinder.bindExpression(parsedExpression);
    ExpressionBinder::implicitCastIfNecessary(whereExpression, LogicalTypeID::BOOL);
    return whereExpression;
}

common::table_id_t Binder::bindTableID(const std::string& tableName) const {
    if (!catalog.containsTable(clientContext->getTx(), tableName)) {
        throw BinderException(common::stringFormat("Table {} does not exist.", tableName));
    }
    return catalog.getTableID(clientContext->getTx(), tableName);
}

std::shared_ptr<Expression> Binder::createVariable(
    std::string_view name, common::LogicalTypeID typeID) {
    return createVariable(std::string(name), LogicalType{typeID});
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
    auto expression = expressionBinder.createVariableExpression(dataType, name);
    expression->setAlias(name);
    scope->addExpression(name, expression);
    return expression;
}

std::unique_ptr<LogicalType> Binder::bindDataType(const std::string& dataType) {
    auto boundType = LogicalTypeUtils::dataTypeFromString(dataType);
    if (boundType.getLogicalTypeID() == LogicalTypeID::FIXED_LIST) {
        auto validNumericTypes = LogicalTypeUtils::getNumericalLogicalTypeIDs();
        auto childType = FixedListType::getChildType(&boundType);
        auto numElementsInList = FixedListType::getNumValuesInList(&boundType);
        if (find(validNumericTypes.begin(), validNumericTypes.end(),
                childType->getLogicalTypeID()) == validNumericTypes.end()) {
            throw BinderException("The child type of a fixed list must be a numeric type. Given: " +
                                  childType->toString() + ".");
        }
        if (numElementsInList == 0) {
            // Note: the parser already guarantees that the number of elements is a non-negative
            // number. However, we still need to check whether the number of elements is 0.
            throw BinderException(
                "The number of elements in a fixed list must be greater than 0. Given: " +
                std::to_string(numElementsInList) + ".");
        }
        auto numElementsPerPage = storage::PageUtils::getNumElementsInAPage(
            storage::StorageUtils::getDataTypeSize(boundType), true /* hasNull */);
        if (numElementsPerPage == 0) {
            throw BinderException(stringFormat("Cannot store a fixed list of size {} in a page.",
                storage::StorageUtils::getDataTypeSize(boundType)));
        }
    }
    return std::make_unique<LogicalType>(boundType);
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
    if (catalog.getTableCatalogEntry(clientContext->getTx(), tableID)->getTableType() !=
        expectedTableType) {
        throw BinderException("Table type mismatch.");
    }
}

void Binder::validateTableExist(const std::string& tableName) {
    if (!catalog.containsTable(clientContext->getTx(), tableName)) {
        throw BinderException("Table " + tableName + " does not exist.");
    }
}

std::string Binder::getUniqueExpressionName(const std::string& name) {
    return "_" + std::to_string(lastExpressionId++) + "_" + name;
}

bool Binder::isReservedPropertyName(const std::string& name) {
    auto normalizedName = StringUtils::getUpper(name);
    if (normalizedName == InternalKeyword::ID) {
        return true;
    }
    if (normalizedName == StringUtils::getUpper(std::string(rdf::PID))) {
        return true;
    }
    return false;
}

std::unique_ptr<BinderScope> Binder::saveScope() {
    return scope->copy();
}

void Binder::restoreScope(std::unique_ptr<BinderScope> prevVariableScope) {
    scope = std::move(prevVariableScope);
}

function::TableFunction* Binder::getScanFunction(FileType fileType, const ReaderConfig& config) {
    function::Function* func;
    auto stringType = LogicalType(LogicalTypeID::STRING);
    std::vector<LogicalType> inputTypes;
    inputTypes.push_back(stringType);
    auto functions = catalog.getBuiltInFunctions(clientContext->getTx());
    switch (fileType) {
    case FileType::PARQUET: {
        func = functions->matchFunction(READ_PARQUET_FUNC_NAME, inputTypes);
    } break;
    case FileType::NPY: {
        func = functions->matchFunction(READ_NPY_FUNC_NAME, inputTypes);
    } break;
    case FileType::CSV: {
        auto csvConfig = CSVReaderConfig::construct(config.options);
        func = functions->matchFunction(
            csvConfig.parallel ? READ_CSV_PARALLEL_FUNC_NAME : READ_CSV_SERIAL_FUNC_NAME,
            inputTypes);
    } break;
    default:
        KU_UNREACHABLE;
    }
    return ku_dynamic_cast<function::Function*, function::TableFunction*>(func);
}

} // namespace binder
} // namespace kuzu
