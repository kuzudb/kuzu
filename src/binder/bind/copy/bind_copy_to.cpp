#include "binder/binder.h"
#include "binder/copy/bound_copy_to.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/function_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
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
    auto fileTypeStr = common::FileTypeUtils::toString(fileType);
    auto name = common::stringFormat("COPY_{}", fileTypeStr);
    auto copyFunc =
        function::BuiltInFunctionsUtils::matchFunction(clientContext->getTx(), name, functions)
            ->constPtrCast<function::CopyFunction>();
    for (auto& column : columns) {
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        columnNames.push_back(columnName);
        // TODO(Xiyang): Query: COPY (RETURN null) TO '/tmp/1.parquet', the datatype of the first
        // column is ANY, should we solve the type at binder?
        columnTypes.push_back(column->getDataType());
    }
    if (fileType != FileType::CSV && copyToStatement.getParsingOptionsRef().size() != 0) {
        throw BinderException{"Only copy to csv can have options."};
    }
    function::CopyFuncBindInput bindInput{std::move(columnNames), std::move(columnTypes),
        std::move(boundFilePath), bindParsingOptions(copyToStatement.getParsingOptionsRef()),
        true /* canParallel */};
    auto bindData = copyFunc->copyToBind(bindInput);
    return std::make_unique<BoundCopyTo>(std::move(bindData), *copyFunc, std::move(query));
}

} // namespace binder
} // namespace kuzu
