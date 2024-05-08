#include "function/table/call_functions.h"
#include "transaction/transaction_manager.h"

namespace kuzu {
namespace function {

// TODO(Guodong): Should return a BOOLEAN column indicating success or not. how about error message?
struct CheckpointBindData final : public CallTableFuncBindData {
    main::ClientContext* clientContext;

    explicit CheckpointBindData(main::ClientContext* clientContext)
        : CallTableFuncBindData({} /*columnTypes*/, {} /*returnColumnNames*/, 1 /*maxOffset*/),
          clientContext(clientContext) {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CheckpointBindData>(clientContext);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput&) {
    auto checkpointBindData =
        common::ku_dynamic_cast<TableFuncBindData*, CheckpointBindData*>(input.bindData);
    checkpointBindData->clientContext->getTransactionManagerUnsafe()->checkpoint(
        *checkpointBindData->clientContext);
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    TableFuncBindInput*) {
    return std::make_unique<CheckpointBindData>(context);
}

function_set CheckpointFunction::getFunctionSet() {
    function_set functionSet;
    std::vector<common::LogicalTypeID> inputTypes;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, inputTypes));
    return functionSet;
}

} // namespace function
} // namespace kuzu
