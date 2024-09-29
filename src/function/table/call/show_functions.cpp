#include "catalog/catalog.h"
#include "function/table/call_functions.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

struct FunctionInfo {
    std::string name;
    std::string type;
    std::string signature;

    FunctionInfo(std::string name, std::string type, std::string signature)
        : name{std::move(name)}, type{std::move(type)}, signature{std::move(signature)} {}
};

struct ShowFunctionsBindData : public CallTableFuncBindData {
    std::vector<FunctionInfo> sequences;

    ShowFunctionsBindData(std::vector<FunctionInfo> sequences, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          sequences{std::move(sequences)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowFunctionsBindData>(sequences, LogicalType::copy(columnTypes),
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
    auto sequences = input.bindData->constPtrCast<ShowFunctionsBindData>()->sequences;
    auto numSequencesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numSequencesToOutput; i++) {
        auto FunctionInfo = sequences[morsel.startOffset + i];
        dataChunk.getValueVectorMutable(0).setValue(i, FunctionInfo.name);
        dataChunk.getValueVectorMutable(1).setValue(i, FunctionInfo.type);
        dataChunk.getValueVectorMutable(2).setValue(i, FunctionInfo.signature);
    }
    return numSequencesToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    ScanTableFuncBindInput*) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("type");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("signature");
    columnTypes.emplace_back(LogicalType::STRING());
    std::vector<FunctionInfo> FunctionInfos;
    for (const auto& entry : context->getCatalog()->getFunctionEntries(context->getTx())) {
        const auto& functionSet = entry->getFunctionSet();
        const auto type = FunctionEntryTypeUtils::toString(entry->getType());
        for (auto& function : functionSet) {
            auto signature = function->signatureToString();
            FunctionInfos.emplace_back(function->name, type, signature);
        }
    }
    return std::make_unique<ShowFunctionsBindData>(std::move(FunctionInfos), std::move(columnTypes),
        std::move(columnNames), FunctionInfos.size());
}

function_set ShowFunctionsFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
