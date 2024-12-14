#include "common/exception/runtime.h"
#include "function/table/simple_table_functions.h"
#include "graph/graph_entry.h"
#include "processor/execution_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct DropProjectGraphBindData : SimpleTableFuncBindData {
    std::string graphName;

    explicit DropProjectGraphBindData(std::string graphName)
        : SimpleTableFuncBindData{0}, graphName{graphName} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DropProjectGraphBindData>(graphName);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& /*output*/) {
    auto bindData = ku_dynamic_cast<DropProjectGraphBindData*>(input.bindData);
    auto& graphEntrySet = input.context->clientContext->getGraphEntrySetUnsafe();
    if (!graphEntrySet.hasGraph(bindData->graphName)) {
        throw RuntimeException(
            stringFormat("Project graph {} does not exists.", bindData->graphName));
    }
    graphEntrySet.dropGraph(bindData->graphName);
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext*,
    TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    return std::make_unique<DropProjectGraphBindData>(graphName);
}

function_set DropProjectGraphFunction::getFunctionSet() {
    function_set functionSet;
    auto func =
        std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = initSharedState;
    func->initLocalStateFunc = initEmptyLocalState;
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
