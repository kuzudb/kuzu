#include "function/table/call_functions.h"
#include "processor/warning_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ShowWarningsBindData : public CallTableFuncBindData {
    std::vector<processor::WarningInfo> warnings;

    ShowWarningsBindData(std::vector<processor::WarningInfo> warnings,
        std::vector<LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          warnings{std::move(warnings)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowWarningsBindData>(warnings, LogicalType::copy(columnTypes),
            columnNames, maxOffset);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = input.sharedState->ptrCast<CallFuncSharedState>();
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
    ScanTableFuncBindInput*) {
    std::vector<std::string> columnNames{WarningConstants::WARNING_TABLE_COLUMN_NAMES.begin(),
        WarningConstants::WARNING_TABLE_COLUMN_NAMES.end()};
    std::vector<LogicalType> columnTypes{WarningConstants::WARNING_TABLE_COLUMN_DATA_TYPES.begin(),
        WarningConstants::WARNING_TABLE_COLUMN_DATA_TYPES.end()};
    std::vector<processor::WarningInfo> warningInfos;
    for (const auto& warning : context->getWarningContext().getPopulatedWarnings()) {
        warningInfos.emplace_back(warning);
    }
    return std::make_unique<ShowWarningsBindData>(std::move(warningInfos), std::move(columnTypes),
        std::move(columnNames), warningInfos.size());
}

function_set ShowWarningsFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
