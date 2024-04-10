#include "binder/binder.h"
#include "binder/copy/bound_copy_to.h"
#include "common/exception/binder.h"
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
    // TODO: check this.
    auto csvConfig =
        CSVReaderConfig::construct(bindParsingOptions(copyToStatement.getParsingOptionsRef()));
    return std::make_unique<BoundCopyTo>(boundFilePath, fileType, std::move(query),
        csvConfig.option.copy());
}

} // namespace binder
} // namespace kuzu
