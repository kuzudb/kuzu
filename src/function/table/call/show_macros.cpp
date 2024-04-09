#include "catalog/catalog.h"
#include "catalog/catalog_entry/scalar_macro_catalog_entry.h"
#include "function/table/call_functions.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

struct ShowMacrosBindData : public CallTableFuncBindData {
    std::vector<ScalarMacroCatalogEntry*> macros;

    ShowMacrosBindData(std::vector<ScalarMacroCatalogEntry*> macros,
        std::vector<LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          macros{std::move(macros)} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowMacrosBindData>(macros, columnTypes, columnNames, maxOffset);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(input.sharedState);
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto macros =
        ku_dynamic_cast<function::TableFuncBindData*, function::ShowMacrosBindData*>(input.bindData)
            ->macros;
    auto numTablesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numTablesToOutput; i++) {
        auto macroEntry = macros[morsel.startOffset + i];
        dataChunk.getValueVector(0)->setValue(i, macroEntry->getName());
        dataChunk.getValueVector(1)->setValue(i, macroEntry->getComment());
    }
    return numTablesToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    TableFuncBindInput*) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("name");
    columnTypes.emplace_back(*LogicalType::STRING());
    columnNames.emplace_back("comment");
    columnTypes.emplace_back(*LogicalType::STRING());
    std::vector<ScalarMacroCatalogEntry*> macrosEntries;
    auto macroNames = context->getCatalog()->getMacroNames(context->getTx());
    for (auto macroName : macroNames) {
        auto macroEntry = context->getCatalog()->getFunctionEntry(context->getTx(), macroName);
        macrosEntries.push_back(
            ku_dynamic_cast<CatalogEntry*, ScalarMacroCatalogEntry*>(macroEntry));
    }
    return std::make_unique<ShowMacrosBindData>(std::move(macrosEntries), std::move(columnTypes),
        std::move(columnNames), (offset_t)macroNames.size());
}

function_set ShowMacroFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
