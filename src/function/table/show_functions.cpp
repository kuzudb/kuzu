#include "binder/binder.h"
#include "catalog/catalog.h"
#include "function/table/bind_data.h"
#include "function/table/table_function.h"

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

struct ShowFunctionsBindData final : TableFuncBindData {
    std::vector<FunctionInfo> sequences;

    ShowFunctionsBindData(std::vector<FunctionInfo> sequences, binder::expression_vector columns,
        offset_t maxOffset)
        : TableFuncBindData{std::move(columns), maxOffset}, sequences{std::move(sequences)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowFunctionsBindData>(sequences, columns, maxOffset);
    }
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    const auto sharedState = input.sharedState->ptrCast<TableFuncSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    const auto sequences = input.bindData->constPtrCast<ShowFunctionsBindData>()->sequences;
    const auto numSequencesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numSequencesToOutput; i++) {
        const auto functionInfo = sequences[morsel.startOffset + i];
        dataChunk.getValueVectorMutable(0).setValue(i, functionInfo.name);
        dataChunk.getValueVectorMutable(1).setValue(i, functionInfo.type);
        dataChunk.getValueVectorMutable(2).setValue(i, functionInfo.signature);
    }
    return numSequencesToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* context,
    const TableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("type");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("signature");
    columnTypes.emplace_back(LogicalType::STRING());
    std::vector<FunctionInfo> FunctionInfos;
    for (const auto& entry : context->getCatalog()->getFunctionEntries(context->getTransaction())) {
        const auto& functionSet = entry->getFunctionSet();
        const auto type = FunctionEntryTypeUtils::toString(entry->getType());
        for (auto& function : functionSet) {
            auto signature = function->signatureToString();
            FunctionInfos.emplace_back(function->name, type, signature);
        }
    }
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<ShowFunctionsBindData>(std::move(FunctionInfos), columns,
        FunctionInfos.size());
}

function_set ShowFunctionsFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = TableFunction::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
