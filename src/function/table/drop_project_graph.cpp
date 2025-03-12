#include "common/exception/runtime.h"
#include "function/table/bind_data.h"
#include "function/table/standalone_call_function.h"
#include "graph/graph_entry.h"
#include "processor/execution_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct DropProjectedGraphBindData final : TableFuncBindData {
    std::string graphName;

    explicit DropProjectedGraphBindData(std::string graphName)
        : TableFuncBindData{0}, graphName{std::move(graphName)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DropProjectedGraphBindData>(graphName);
    }
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto bindData = ku_dynamic_cast<DropProjectedGraphBindData*>(input.bindData);
    auto& graphEntrySet = input.context->clientContext->getGraphEntrySetUnsafe();
    if (!graphEntrySet.hasGraph(bindData->graphName)) {
        throw RuntimeException(
            stringFormat("Project graph {} does not exists.", bindData->graphName));
    }
    graphEntrySet.dropGraph(bindData->graphName);
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext*,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0 /* maxOffset */);
    return std::make_unique<DropProjectedGraphBindData>(graphName);
}

function_set DropProjectedGraphFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = TableFunction::initEmptySharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
