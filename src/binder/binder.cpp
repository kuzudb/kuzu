#include "binder/binder.h"

#include "binder/bound_statement_rewriter.h"
#include "catalog/catalog.h"
#include "common/copier_config/csv_reader_config.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "function/built_in_function_utils.h"
#include "function/table_functions.h"
#include "processor/operator/persistent/reader/csv/parallel_csv_reader.h"
#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"
#include "processor/operator/persistent/reader/npy/npy_reader.h"
#include "processor/operator/persistent/reader/parquet/parquet_reader.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::parser;
using namespace kuzu::processor;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bind(const Statement& statement) {
    std::unique_ptr<BoundStatement> boundStatement;
    switch (statement.getStatementType()) {
    case StatementType::CREATE_TABLE: {
        boundStatement = bindCreateTable(statement);
    } break;
    case StatementType::CREATE_TYPE: {
        boundStatement = bindCreateType(statement);
    } break;
    case StatementType::CREATE_SEQUENCE: {
        boundStatement = bindCreateSequence(statement);
    } break;
    case StatementType::COPY_FROM: {
        boundStatement = bindCopyFromClause(statement);
    } break;
    case StatementType::COPY_TO: {
        boundStatement = bindCopyToClause(statement);
    } break;
    case StatementType::DROP: {
        boundStatement = bindDrop(statement);
    } break;
    case StatementType::ALTER: {
        boundStatement = bindAlter(statement);
    } break;
    case StatementType::QUERY: {
        boundStatement = bindQuery(statement);
    } break;
    case StatementType::STANDALONE_CALL: {
        boundStatement = bindStandaloneCall(statement);
    } break;
    case StatementType::STANDALONE_CALL_FUNCTION: {
        boundStatement = bindStandaloneCallFunction(statement);
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
    case StatementType::IMPORT_DATABASE: {
        boundStatement = bindImportDatabaseClause(statement);
    } break;
    case StatementType::ATTACH_DATABASE: {
        boundStatement = bindAttachDatabase(statement);
    } break;
    case StatementType::DETACH_DATABASE: {
        boundStatement = bindDetachDatabase(statement);
    } break;
    case StatementType::USE_DATABASE: {
        boundStatement = bindUseDatabase(statement);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    BoundStatementRewriter::rewrite(*boundStatement, *clientContext);
    return boundStatement;
}

std::shared_ptr<Expression> Binder::bindWhereExpression(const ParsedExpression& parsedExpression) {
    auto whereExpression = expressionBinder.bindExpression(parsedExpression);
    expressionBinder.implicitCastIfNecessary(whereExpression, LogicalType::BOOL());
    return whereExpression;
}

std::shared_ptr<Expression> Binder::createVariable(std::string_view name, LogicalTypeID typeID) {
    return createVariable(std::string(name), LogicalType{typeID});
}

std::shared_ptr<Expression> Binder::createVariable(const std::string& name,
    LogicalTypeID logicalTypeID) {
    return createVariable(name, LogicalType{logicalTypeID});
}

std::shared_ptr<Expression> Binder::createVariable(const std::string& name,
    const LogicalType& dataType) {
    if (scope.contains(name)) {
        throw BinderException("Variable " + name + " already exists.");
    }
    auto expression = expressionBinder.createVariableExpression(dataType.copy(), name);
    expression->setAlias(name);
    addToScope(name, expression);
    return expression;
}

std::shared_ptr<Expression> Binder::createInvisibleVariable(const std::string& name,
    const LogicalType& dataType) const {
    auto expression = expressionBinder.createVariableExpression(dataType.copy(), name);
    expression->setAlias(name);
    return expression;
}

expression_vector Binder::createVariables(const std::vector<std::string>& names,
    const std::vector<LogicalType>& types) {
    KU_ASSERT(names.size() == types.size());
    expression_vector variables;
    for (auto i = 0u; i < names.size(); ++i) {
        variables.push_back(createVariable(names[i], types[i]));
    }
    return variables;
}

expression_vector Binder::createInvisibleVariables(const std::vector<std::string>& names,
    const std::vector<LogicalType>& types) const {
    KU_ASSERT(names.size() == types.size());
    expression_vector variables;
    for (auto i = 0u; i < names.size(); ++i) {
        variables.push_back(createInvisibleVariable(names[i], types[i]));
    }
    return variables;
}

void Binder::validateOrderByFollowedBySkipOrLimitInWithClause(
    const BoundProjectionBody& boundProjectionBody) {
    auto hasSkipOrLimit = boundProjectionBody.hasSkip() || boundProjectionBody.hasLimit();
    if (boundProjectionBody.hasOrderByExpressions() && !hasSkipOrLimit) {
        throw BinderException("In WITH clause, ORDER BY must be followed by SKIP or LIMIT.");
    }
}

void Binder::validateTableExist(const std::string& tableName) const {
    if (!clientContext->getCatalog()->containsTable(clientContext->getTransaction(), tableName,
            clientContext->shouldUseInternalCatalogEntry())) {
        throw BinderException("Table " + tableName + " does not exist.");
    }
}

std::string Binder::getUniqueExpressionName(const std::string& name) {
    return "_" + std::to_string(lastExpressionId++) + "_" + name;
}

struct ReservedNames {
    // Column name that might conflict with internal names.
    static std::unordered_set<std::string> getColumnNames() {
        return {
            InternalKeyword::ID,
            InternalKeyword::LABEL,
            InternalKeyword::SRC,
            InternalKeyword::DST,
            InternalKeyword::DIRECTION,
            InternalKeyword::LENGTH,
            InternalKeyword::NODES,
            InternalKeyword::RELS,
            InternalKeyword::PLACE_HOLDER,
            StringUtils::getUpper(InternalKeyword::ROW_OFFSET),
            StringUtils::getUpper(InternalKeyword::SRC_OFFSET),
            StringUtils::getUpper(InternalKeyword::DST_OFFSET),
        };
    }

    // Properties that should be hidden from user access.
    static std::unordered_set<std::string> getPropertyLookupName() {
        return {
            InternalKeyword::ID,
        };
    }
};

bool Binder::reservedInColumnName(const std::string& name) {
    auto normalizedName = StringUtils::getUpper(name);
    return ReservedNames::getColumnNames().contains(normalizedName);
}

bool Binder::reservedInPropertyLookup(const std::string& name) {
    auto normalizedName = StringUtils::getUpper(name);
    return ReservedNames::getPropertyLookupName().contains(normalizedName);
}

void Binder::addToScope(const std::vector<std::string>& names, const expression_vector& exprs) {
    KU_ASSERT(names.size() == exprs.size());
    for (auto i = 0u; i < names.size(); ++i) {
        addToScope(names[i], exprs[i]);
    }
}

void Binder::addToScope(const std::string& name, std::shared_ptr<Expression> expr) {
    // TODO(Xiyang): assert name not in scope.
    // Note to Xiyang: I don't think the TODO still stands here. I tried adding the assertion, but
    // it failed a few tests. You may want to revisit this TODO.
    scope.addExpression(name, std::move(expr));
}

BinderScope Binder::saveScope() const {
    return scope.copy();
}

void Binder::restoreScope(BinderScope prevScope) {
    scope = std::move(prevScope);
}

TableFunction Binder::getScanFunction(const FileTypeInfo& typeInfo,
    const FileScanInfo& fileScanInfo) const {
    Function* func = nullptr;
    std::vector<LogicalType> inputTypes;
    inputTypes.push_back(LogicalType::STRING());
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTransaction();
    switch (typeInfo.fileType) {
    case FileType::PARQUET: {
        auto entry = catalog->getFunctionEntry(transaction, ParquetScanFunction::name);
        func = BuiltInFunctionsUtils::matchFunction(ParquetScanFunction::name, inputTypes,
            entry->ptrCast<FunctionCatalogEntry>());
    } break;
    case FileType::NPY: {
        auto entry = catalog->getFunctionEntry(transaction, NpyScanFunction::name);
        func = BuiltInFunctionsUtils::matchFunction(NpyScanFunction::name, inputTypes,
            entry->ptrCast<FunctionCatalogEntry>());
    } break;
    case FileType::CSV: {
        auto csvConfig = CSVReaderConfig::construct(fileScanInfo.options);
        auto name = csvConfig.parallel ? ParallelCSVScan::name : SerialCSVScan::name;
        auto entry = catalog->getFunctionEntry(transaction, name);
        func = BuiltInFunctionsUtils::matchFunction(name, inputTypes,
            entry->ptrCast<FunctionCatalogEntry>());
    } break;
    case FileType::UNKNOWN: {
        try {
            auto name = stringFormat("{}_SCAN", typeInfo.fileTypeStr);
            auto entry = catalog->getFunctionEntry(transaction, name);
            func = BuiltInFunctionsUtils::matchFunction(name, inputTypes,
                entry->ptrCast<FunctionCatalogEntry>());
        } catch (...) {
            if (typeInfo.fileTypeStr == "") {
                throw BinderException{"Cannot infer the format of the given file. Please "
                                      "set the file format explicitly by (file_format=<type>)."};
            }
            throw BinderException{
                stringFormat("Cannot load from file type {}.", typeInfo.fileTypeStr)};
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    return *func->ptrCast<TableFunction>();
}

} // namespace binder
} // namespace kuzu
