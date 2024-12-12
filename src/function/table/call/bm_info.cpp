#include "function/table/simple_table_functions.h"
#include "main/database.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace function {

struct BMInfoBindData final : SimpleTableFuncBindData {
    uint64_t memLimit;
    uint64_t memUsage;

    BMInfoBindData(uint64_t memLimit, uint64_t memUsage,
        std::vector<common::LogicalType> returnTypes, std::vector<std::string> returnColumnNames)
        : SimpleTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), 1},
          memLimit{memLimit}, memUsage{memUsage} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<BMInfoBindData>(memLimit, memUsage,
            common::LogicalType::copy(columnTypes), columnNames);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    KU_ASSERT(output.dataChunk.getNumValueVectors() == 2);
    const auto sharedState = input.sharedState->ptrCast<SimpleTableFuncSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    const auto bindData = input.bindData->constPtrCast<BMInfoBindData>();
    output.dataChunk.getValueVectorMutable(0).setValue<uint64_t>(0, bindData->memLimit);
    output.dataChunk.getValueVectorMutable(1).setValue<uint64_t>(0, bindData->memUsage);
    return 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    TableFuncBindInput*) {
    auto memLimit = context->getMemoryManager()->getBufferManager()->getMemoryLimit();
    auto memUsage = context->getMemoryManager()->getBufferManager()->getUsedMemory();
    std::vector<common::LogicalType> returnTypes;
    returnTypes.emplace_back(common::LogicalType::UINT64());
    returnTypes.emplace_back(common::LogicalType::UINT64());
    auto returnColumnNames = std::vector<std::string>{"mem_limit", "mem_usage"};
    return std::make_unique<BMInfoBindData>(memLimit, memUsage, std::move(returnTypes),
        std::move(returnColumnNames));
}

function_set BMInfoFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<common::LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
