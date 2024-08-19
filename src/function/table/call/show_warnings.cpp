#include "function/table/call_functions.h"
#include "processor/operator/persistent/reader/csv/csv_error.h"
#include "processor/warning_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct WarningInfo {
    uint64_t queryID;
    processor::PopulatedCSVError warning;

    explicit WarningInfo(processor::PopulatedCSVError warning)
        : queryID(0), warning(std::move(warning)) {}
};

struct ShowWarningsBindData : public CallTableFuncBindData {
    std::vector<WarningInfo> warnings;

    ShowWarningsBindData(std::vector<WarningInfo> warnings, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
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
        dataChunk.getValueVector(0)->setValue(i, tableInfo.queryID);
        dataChunk.getValueVector(1)->setValue(i, tableInfo.warning.message);
        dataChunk.getValueVector(2)->setValue(i, tableInfo.warning.filePath);
        dataChunk.getValueVector(3)->setValue(i, tableInfo.warning.lineNumber);
        dataChunk.getValueVector(4)->setValue(i, tableInfo.warning.reconstructedLine);
    }
    return numWarningsToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    TableFuncBindInput*) {
    std::vector<std::string> columnNames = processor::WarningSchema::getColumnNames();
    std::vector<LogicalType> columnTypes = processor::WarningSchema::getColumnDataTypes();
    std::vector<WarningInfo> warningInfos;
    for (const auto& warning : context->getWarningContext().warnings) {
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
