#include "function/table_functions/db_version.h"

#include "common/vector/value_vector.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
void DBVersionFunction::tableFunc(std::pair<offset_t, offset_t> morsel,
    function::TableFuncBindData* bindData, std::vector<ValueVector*> outputVectors) {
    auto outputVector = outputVectors[0];
    auto pos = outputVector->state->selVector->selectedPositions[0];
    outputVectors[0]->setValue(pos, std::string(KUZU_VERSION));
    outputVectors[0]->setNull(pos, false);
    outputVector->state->selVector->selectedSize = 1;
}

std::unique_ptr<TableFuncBindData> DBVersionFunction::bindFunc(main::ClientContext* context,
    kuzu::function::TableFuncBindInput input, catalog::CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("version");
    returnTypes.emplace_back(LogicalTypeID::STRING);
    return std::make_unique<TableFuncBindData>(
        std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}

} // namespace function
} // namespace kuzu
