#include "function/table/call_functions.h"
#include "transaction/transaction_manager.h"

namespace kuzu {
namespace function {

struct CheckpointBindData final : CallTableFuncBindData {
    main::ClientContext* clientContext;

    explicit CheckpointBindData(main::ClientContext* clientContext)
        : CallTableFuncBindData({} /*columnTypes*/, {} /*returnColumnNames*/, 1 /*maxOffset*/),
          clientContext(clientContext) {
        columnTypes.push_back(common::LogicalType::BOOL());
        columnNames.push_back("result");
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CheckpointBindData>(clientContext);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput&) {
    auto sharedState = input.sharedState->ptrCast<CallFuncSharedState>();
    auto checkpointBindData =
        common::ku_dynamic_cast<TableFuncBindData*, CheckpointBindData*>(input.bindData);
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    KU_ASSERT(checkpointBindData->clientContext->getTransactionContext()->hasActiveTransaction());
    checkpointBindData->clientContext->getTransactionContext()->commit();
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
