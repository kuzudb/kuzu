#include "binder/binder.h"
#include "binder/copy/bound_copy_to.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/function_catalog_entry.h"
#include "common/exception/binder.h"
#include "function/built_in_function_utils.h"
#include "parser/copy.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyToClause(const Statement& statement) {
    auto& copyToStatement = statement.constCast<CopyTo>();
    auto boundFilePath = copyToStatement.getFilePath();
    auto fileType = bindFileType(boundFilePath);
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    auto parsedQuery = copyToStatement.getStatement()->constPtrCast<RegularQuery>();
    auto query = bindQuery(*parsedQuery);
    auto columns = query->getStatementResult()->getColumns();

    auto functions = clientContext->getCatalog()->getFunctions(clientContext->getTx());
    std::string name = "COPY_{}";
    switch (fileType) {
    case FileType::CSV:
        name = stringFormat(name, "CSV");
        break;
    case FileType::PARQUET:
        name = stringFormat(name, "PARQUET");
        break;
    default:
        KU_UNREACHABLE;
    }
    auto copyFunc =
        *function::BuiltInFunctionsUtils::matchFunction(clientContext->getTx(), name, functions)
             ->constPtrCast<function::CopyFunction>();
    for (auto& column : columns) {
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        columnNames.push_back(columnName);
        columnTypes.push_back(column->getDataType());
    }
    if (fileType != FileType::CSV && fileType != FileType::PARQUET) {
        throw BinderException("COPY TO currently only supports csv and parquet files.");
    }
    if (fileType != FileType::CSV && copyToStatement.getParsingOptionsRef().size() != 0) {
        throw BinderException{"Only copy to csv can have options."};
    }
    function::CopyFuncBindInput bindInput{std::move(columnNames), std::move(columnTypes),
        std::move(boundFilePath), bindParsingOptions(copyToStatement.getParsingOptionsRef()),
        true /* canParallel */};
    auto bindData = copyFunc.copyToBind(bindInput);
    return std::make_unique<BoundCopyTo>(std::move(bindData), std::move(copyFunc),
        std::move(query));
}

} // namespace binder
} // namespace kuzu
