#include "catalog/catalog.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "function/table/call_functions.h"
#include "main/database_manager.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

struct TableInfo {
    std::string name;
    std::string type;
    std::string databaseName;
    std::string comment;

    TableInfo(std::string name, std::string type, std::string databaseName, std::string comment)
        : name{std::move(name)}, type{std::move(type)}, databaseName{std::move(databaseName)},
          comment{std::move(comment)} {}
};

struct ShowTablesBindData : public CallTableFuncBindData {
    std::vector<TableInfo> tables;

    ShowTablesBindData(std::vector<TableInfo> tables, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          tables{std::move(tables)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowTablesBindData>(tables, columnTypes, columnNames, maxOffset);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = input.sharedState->ptrCast<CallFuncSharedState>();
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto tables = input.bindData->constPtrCast<ShowTablesBindData>()->tables;
    auto numTablesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numTablesToOutput; i++) {
        auto tableInfo = tables[morsel.startOffset + i];
        dataChunk.getValueVector(0)->setValue(i, tableInfo.name);
        dataChunk.getValueVector(1)->setValue(i, tableInfo.type);
        dataChunk.getValueVector(2)->setValue(i, tableInfo.databaseName);
        dataChunk.getValueVector(3)->setValue(i, tableInfo.comment);
    }
    return numTablesToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    TableFuncBindInput*) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("name");
    columnTypes.emplace_back(*LogicalType::STRING());
    columnNames.emplace_back("type");
    columnTypes.emplace_back(*LogicalType::STRING());
    columnNames.emplace_back("database name");
    columnTypes.emplace_back(*LogicalType::STRING());
    columnNames.emplace_back("comment");
    columnTypes.emplace_back(*LogicalType::STRING());
    std::vector<TableInfo> tableInfos;
    if (!context->hasDefaultDatabase()) {
        for (auto& entry : context->getCatalog()->getTableEntries(context->getTx())) {
            auto tableInfo =
                TableInfo{entry->getName(), TableTypeUtils::toString(entry->getTableType()),
                    LOCAL_DB_NAME, entry->getComment()};
            tableInfos.push_back(std::move(tableInfo));
        }
    }

    auto databaseManager = context->getDatabaseManager();
    for (auto attachedDatabase : databaseManager->getAttachedDatabases()) {
        auto databaseName = attachedDatabase->getDBName();
        auto databaseType = attachedDatabase->getDBType();
        for (auto& entry : attachedDatabase->getCatalog()->getTableEntries(context->getTx())) {
            auto tableInfo =
                TableInfo{entry->getName(), TableTypeUtils::toString(entry->getTableType()),
                    stringFormat("{}({})", databaseName, databaseType), entry->getComment()};
            tableInfos.push_back(std::move(tableInfo));
        }
    }
    return std::make_unique<ShowTablesBindData>(std::move(tableInfos), std::move(columnTypes),
        std::move(columnNames), tableInfos.size());
}

function_set ShowTablesFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
