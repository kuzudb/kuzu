#include "function/iceberg_functions.h"

namespace kuzu {
namespace iceberg_extension {

using namespace function;
using namespace common;

std::unique_ptr<TableFuncBindData> metadataBindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    return bindFuncHelper(context, input, IcebergMetadataFunction::name);
}

function_set IcebergMetadataFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = delta_extension::tableFunc;
    function->bindFunc = metadataBindFunc;
    function->initSharedStateFunc = delta_extension::initDeltaScanSharedState;
    function->initLocalStateFunc = delta_extension::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace iceberg_extension
} // namespace kuzu
