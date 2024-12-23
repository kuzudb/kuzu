#include "binder/binder.h"
#include "function/table/simple_table_functions.h"
#include "processor/warning_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ShowWarningsBindData : public SimpleTableFuncBindData {
    std::vector<processor::WarningInfo> warnings;

    ShowWarningsBindData(std::vector<processor::WarningInfo> warnings,
        binder::expression_vector columns, offset_t maxOffset)
        : SimpleTableFuncBindData{std::move(columns), maxOffset}, warnings{std::move(warnings)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowWarningsBindData>(warnings, columns, maxOffset);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = input.sharedState->ptrCast<SimpleTableFuncSharedState>();
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto warnings = input.bindData->constPtrCast<ShowWarningsBindData>()->warnings;
    auto numWarningsToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numWarningsToOutput; i++) {
        auto tableInfo = warnings[morsel.startOffset + i];
        dataChunk.getValueVectorMutable(0).setValue(i, tableInfo.queryID);
        dataChunk.getValueVectorMutable(1).setValue(i, tableInfo.warning.message);
        dataChunk.getValueVectorMutable(2).setValue(i, tableInfo.warning.filePath);
        dataChunk.getValueVectorMutable(3).setValue(i, tableInfo.warning.lineNumber);
        dataChunk.getValueVectorMutable(4).setValue(i, tableInfo.warning.skippedLineOrRecord);
    }
    return numWarningsToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    TableFuncBindInput* input) {
    std::vector<std::string> columnNames{WarningConstants::WARNING_TABLE_COLUMN_NAMES.begin(),
        WarningConstants::WARNING_TABLE_COLUMN_NAMES.end()};
    std::vector<LogicalType> columnTypes{WarningConstants::WARNING_TABLE_COLUMN_DATA_TYPES.begin(),
        WarningConstants::WARNING_TABLE_COLUMN_DATA_TYPES.end()};
    std::vector<processor::WarningInfo> warningInfos;
    for (const auto& warning : context->getWarningContext().getPopulatedWarnings()) {
        warningInfos.emplace_back(warning);
    }
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<ShowWarningsBindData>(std::move(warningInfos), columns,
        warningInfos.size());
}

function_set ShowWarningsFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initSharedState;
    function->initLocalStateFunc = initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
