#include "binder/binder.h"
#include "binder/bound_scan_source.h"
#include "binder/expression/expression_util.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "common/exception/binder.h"
#include "parser/query/reading_clause/load_from.h"
#include "parser/scan_source.h"

using namespace kuzu::function;
using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindLoadFrom(const ReadingClause& readingClause) {
    auto& loadFrom = readingClause.constCast<LoadFrom>();
    auto source = loadFrom.getSource();
    std::unique_ptr<BoundLoadFrom> boundLoadFrom;
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    for (auto& [name, type] : loadFrom.getColumnDefinitions()) {
        columnNames.push_back(name);
        columnTypes.push_back(LogicalType::convertFromString(type, clientContext));
    }
    switch (source->type) {
    case ScanSourceType::OBJECT: {
        auto objectSource = source->ptrCast<ObjectScanSource>();
        auto boundScanSource = bindObjectScanSource(*objectSource, loadFrom.getParsingOptions(),
            columnNames, columnTypes);
        auto& scanInfo = boundScanSource->constCast<BoundTableScanSource>().info;
        boundLoadFrom = std::make_unique<BoundLoadFrom>(scanInfo.copy());
    } break;
    case ScanSourceType::FILE: {
        auto fileSource = source->constPtrCast<FileScanSource>();
        auto filePaths = bindFilePaths(fileSource->filePaths);
        if (filePaths.size() > 1) {
            throw BinderException("Load from multiple files is not supported.");
        }
        auto fileTypeInfo = bindFileTypeInfo(filePaths);
        auto boundScanSource =
            bindFileScanSource(*source, loadFrom.getParsingOptions(), columnNames, columnTypes);
        auto& scanInfo = boundScanSource->constCast<BoundTableScanSource>().info;
        boundLoadFrom = std::make_unique<BoundLoadFrom>(scanInfo.copy());
    } break;
    default:
        throw BinderException(stringFormat("LOAD FROM subquery is not supported."));
    }
    if (!columnTypes.empty()) {
        auto info = boundLoadFrom->getInfo();
        for (auto i = 0u; i < columnTypes.size(); ++i) {
            ExpressionUtil::validateDataType(*info->columns[i], columnTypes[i]);
        }
    }
    if (loadFrom.hasWherePredicate()) {
        auto wherePredicate = bindWhereExpression(*loadFrom.getWherePredicate());
        boundLoadFrom->setPredicate(std::move(wherePredicate));
    }
    return boundLoadFrom;
}

} // namespace binder
} // namespace kuzu
