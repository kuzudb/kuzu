#include "binder/binder.h"
#include "binder/bound_scan_source.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_utils.h"
#include "main/database.h"
#include "main/database_manager.h"
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
    for (auto& [name, type] : loadFrom.getPropertyDefinitions()) {
        columnNames.push_back(name);
        columnTypes.push_back(clientContext->getCatalog()->getType(clientContext->getTx(), type));
    }
    switch (source->type) {
    case ScanSourceType::OBJECT: {
        auto objectSource = source->ptrCast<ObjectScanSource>();
        auto boundScanSource = bindObjectScanSource(*objectSource, columnNames, columnTypes);
        auto& scanInfo = boundScanSource->constCast<BoundTableScanSource>().info;
        boundLoadFrom = std::make_unique<BoundLoadFrom>(scanInfo.copy());
    } break;
    case ScanSourceType::FILE: {
        auto fileSource = source->constPtrCast<FileScanSource>();
        auto filePaths = bindFilePaths(fileSource->filePaths);
        if (filePaths.size() > 1) {
            throw BinderException("Load from multiple files is not supported.");
        }
        auto fileType = bindFileType(filePaths);
        switch (fileType) {
        case common::FileType::CSV:
        case common::FileType::PARQUET:
        case common::FileType::NPY:
        case common::FileType::JSON:
            break;
        default:
            throw BinderException(
                stringFormat("Cannot load from file type {}.", FileTypeUtils::toString(fileType)));
        }
        auto boundScanSource =
            bindFileScanSource(*source, loadFrom.getParsingOptions(), columnNames, columnTypes);
        auto& scanInfo = boundScanSource->constCast<BoundTableScanSource>().info;
        boundLoadFrom = std::make_unique<BoundLoadFrom>(scanInfo.copy());
    } break;
    default:
        throw BinderException(stringFormat("LOAD FROM subquery is not supported."));
    }
    if (loadFrom.hasWherePredicate()) {
        auto wherePredicate = bindWhereExpression(*loadFrom.getWherePredicate());
        boundLoadFrom->setPredicate(std::move(wherePredicate));
    }
    return boundLoadFrom;
}

} // namespace binder
} // namespace kuzu
