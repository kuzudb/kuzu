#include "catalog/catalog.h"
#include "function/table/call_functions.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

struct ShowTablesBindData : public CallTableFuncBindData {
    std::vector<TableCatalogEntry*> tables;

    ShowTablesBindData(std::vector<TableCatalogEntry*> tables, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          tables{std::move(tables)} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowTablesBindData>(tables, columnTypes, columnNames, maxOffset);
    }
};

static void tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(input.sharedState);
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        outputChunk.getValueVector(0)->state->selVector->selectedSize = 0;
        return;
    }
    auto tables =
        ku_dynamic_cast<function::TableFuncBindData*, function::ShowTablesBindData*>(input.bindData)
            ->tables;
    auto numTablesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numTablesToOutput; i++) {
        auto tableEntry = tables[morsel.startOffset + i];
        outputChunk.getValueVector(0)->setValue(i, tableEntry->getName());
        std::string typeString = TableTypeUtils::toString(tableEntry->getTableType());
        outputChunk.getValueVector(1)->setValue(i, typeString);
        outputChunk.getValueVector(2)->setValue(i, tableEntry->getComment());
    }
    outputChunk.state->selVector->selectedSize = numTablesToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    main::ClientContext* context, TableFuncBindInput*) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("name");
    columnTypes.emplace_back(*LogicalType::STRING());
    columnNames.emplace_back("type");
    columnTypes.emplace_back(*LogicalType::STRING());
    columnNames.emplace_back("comment");
    columnTypes.emplace_back(*LogicalType::STRING());
    auto tableEntries = context->getCatalog()->getTableEntries(context->getTx());
    auto numTables = context->getCatalog()->getTableCount(context->getTx());
    return std::make_unique<ShowTablesBindData>(
        std::move(tableEntries), std::move(columnTypes), std::move(columnNames), numTables);
}

function_set ShowTablesFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(SHOW_TABLES_FUNC_NAME, tableFunc,
        bindFunc, initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
