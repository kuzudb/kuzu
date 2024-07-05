#include "binder/binder.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_utils.h"
#include "main/database.h"
#include "main/database_manager.h"
#include "parser/query/reading_clause/load_from.h"

using namespace kuzu::function;
using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

static TableFunction getObjectScanFunc(const ObjectScanSource* objectSource,
    main::ClientContext* clientContext) {
    // Bind external database table
    auto dbName = objectSource->objectNames[0];
    auto attachedDB = clientContext->getDatabaseManager()->getAttachedDatabase(dbName);
    if (attachedDB == nullptr) {
        throw BinderException{stringFormat("No database named {} has been attached.", dbName)};
    }
    auto tableName = objectSource->objectNames[1];
    auto attachedCatalog = attachedDB->getCatalog();
    auto tableID = attachedCatalog->getTableID(clientContext->getTx(), tableName);
    auto entry = attachedCatalog->getTableCatalogEntry(clientContext->getTx(), tableID);
    auto tableEntry = ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry);
    return tableEntry->getScanFunction();
}

std::unique_ptr<BoundReadingClause> Binder::bindLoadFrom(const ReadingClause& readingClause) {
    auto& loadFrom = readingClause.constCast<LoadFrom>();
    TableFunction scanFunction;
    std::unique_ptr<TableFuncBindData> bindData;
    auto source = loadFrom.getSource();
    switch (source->type) {
    case ScanSourceType::OBJECT: {
        auto objectSource = reinterpret_cast<ObjectScanSource*>(source);
        KU_ASSERT(!objectSource->objectNames.empty());
        if (objectSource->objectNames.size() == 1) {
            // Bind external object as table
            auto objectName = objectSource->objectNames[0];
            auto replacementData = clientContext->tryReplace(objectName);
            if (replacementData != nullptr) {
                scanFunction = replacementData->func;
                bindData = scanFunction.bindFunc(clientContext, &replacementData->bindInput);
            } else if (clientContext->getDatabaseManager()->hasDefaultDatabase()) {
                objectSource->objectNames.insert(objectSource->objectNames.begin(),
                    clientContext->getDatabaseManager()->getDefaultDatabase());
                scanFunction = getObjectScanFunc(objectSource, clientContext);
                auto bindInput = function::TableFuncBindInput();
                bindData = scanFunction.bindFunc(clientContext, &bindInput);
            } else {
                throw BinderException(ExceptionMessage::variableNotInScope(objectName));
            }
        } else if (objectSource->objectNames.size() == 2) {
            // Bind external database table
            scanFunction = getObjectScanFunc(objectSource, clientContext);
            auto bindInput = function::TableFuncBindInput();
            bindData = scanFunction.bindFunc(clientContext, &bindInput);
        } else {
            // LCOV_EXCL_START
            // The following should never happen.
            auto objectStr = StringUtils::join(objectSource->objectNames, ",");
            throw BinderException(stringFormat("Cannot find object {}.", objectStr));
            // LCOV_EXCL_STOP
        }
    } break;
    case ScanSourceType::FILE: {
        auto fileSource = source->constPtrCast<FileScanSource>();
        auto filePaths = bindFilePaths(fileSource->filePaths);
        auto fileType = bindFileType(filePaths);
        auto readerConfig = std::make_unique<ReaderConfig>(fileType, std::move(filePaths));
        readerConfig->options = bindParsingOptions(loadFrom.getParsingOptionsRef());
        if (readerConfig->getNumFiles() > 1) {
            throw BinderException("Load from multiple files is not supported.");
        }
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
        // Bind columns from input.
        std::vector<std::string> expectedColumnNames;
        std::vector<LogicalType> expectedColumnTypes;
        for (auto& [name, type] : loadFrom.getPropertyDefinitionsRef()) {
            expectedColumnNames.push_back(name);
            expectedColumnTypes.push_back(
                clientContext->getCatalog()->getType(clientContext->getTx(), type));
        }
        scanFunction = getScanFunction(readerConfig->fileType, *readerConfig);
        auto bindInput = ScanTableFuncBindInput(readerConfig->copy(),
            std::move(expectedColumnNames), std::move(expectedColumnTypes), clientContext);
        bindData = scanFunction.bindFunc(clientContext, &bindInput);
    } break;
    default:
        throw BinderException(stringFormat("LOAD FROM subquery is not supported."));
    }
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], bindData->columnTypes[i]));
    }
    auto info = BoundFileScanInfo(scanFunction, std::move(bindData), std::move(columns));
    auto boundLoadFrom = std::make_unique<BoundLoadFrom>(std::move(info));
    if (loadFrom.hasWherePredicate()) {
        auto wherePredicate = bindWhereExpression(*loadFrom.getWherePredicate());
        boundLoadFrom->setPredicate(std::move(wherePredicate));
    }
    return boundLoadFrom;
}

} // namespace binder
} // namespace kuzu
