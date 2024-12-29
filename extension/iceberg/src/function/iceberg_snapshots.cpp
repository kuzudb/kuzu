#include "function/iceberg_functions.h"

namespace kuzu {
namespace iceberg_extension {

using namespace function;
using namespace common;

static std::unique_ptr<TableFuncBindData> snapshotBindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    return bindFuncHelper(context, input, IcebergSnapshotsFunction::name);
}

function_set IcebergSnapshotsFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = delta_extension::tableFunc;
    function->bindFunc = snapshotBindFunc;
    function->initSharedStateFunc = delta_extension::initDeltaScanSharedState;
    function->initLocalStateFunc = delta_extension::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace iceberg_extension
} // namespace kuzu
