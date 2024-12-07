#include "function/iceberg_scan.h"

#include "connector/connector_factory.h"
#include "connector/duckdb_result_converter.h"
#include "connector/duckdb_type_converter.h"
#include "function/delta_scan.h"
#include "main/delta_extension.h"

namespace kuzu {
namespace iceberg_extension {

using namespace function;
using namespace common;

function_set IcebergScanFunction::getFunctionSet() {
    function_set functionSet;
    // Use the shared `bindFuncInternal` from delta_scan and pass "ICEBERG" as the scan type.
    auto icebergBindFunc = [](main::ClientContext* context, ScanTableFuncBindInput* input) {
        return delta_extension::bindFuncInternal(context, input, "ICEBERG");
    };
    functionSet.push_back(std::make_unique<TableFunction>(name, delta_extension::tableFunc,
        icebergBindFunc, delta_extension::initDeltaScanSharedState, initEmptyLocalState,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

} // namespace iceberg_extension
} // namespace kuzu
