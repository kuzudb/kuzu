#include "function/table/simple_table_function.h"

#include "function/table/bind_data.h"

namespace kuzu {
namespace function {

TableFuncMorsel SimpleTableFuncSharedState::getMorsel() {
    std::lock_guard lck{mtx};
    KU_ASSERT(curRowIdx <= numRows);
    if (curRowIdx == numRows) {
        return TableFuncMorsel::createInvalidMorsel();
    }
    const auto numValuesToOutput = std::min(maxMorselSize, numRows - curRowIdx);
    curRowIdx += numValuesToOutput;
    return {curRowIdx - numValuesToOutput, curRowIdx};
}

std::unique_ptr<TableFuncSharedState> SimpleTableFunc::initSharedState(
    const TableFunctionInitInput& input) {
    return std::make_unique<SimpleTableFuncSharedState>(input.bindData->numRows);
}

} // namespace function
} // namespace kuzu
