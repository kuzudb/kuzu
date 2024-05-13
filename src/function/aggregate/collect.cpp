#include "function/aggregate/collect.h"

#include "storage/storage_utils.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

std::unique_ptr<AggregateState> CollectFunction::initialize() {
    return std::make_unique<CollectState>();
}

void CollectFunction::updateAll(uint8_t* state_, ValueVector* input, uint64_t multiplicity,
    MemoryManager* memoryManager) {
    KU_ASSERT(!input->state->isFlat());
    auto state = reinterpret_cast<CollectState*>(state_);
    auto& inputSelVector = input->state->getSelVector();
    if (input->hasNoNullsGuarantee()) {
        for (auto i = 0u; i < inputSelVector.getSelSize(); ++i) {
            auto pos = inputSelVector[i];
            updateSingleValue(state, input, pos, multiplicity, memoryManager);
        }
    } else {
        for (auto i = 0u; i < inputSelVector.getSelSize(); ++i) {
            auto pos = inputSelVector[i];
            if (!input->isNull(pos)) {
                updateSingleValue(state, input, pos, multiplicity, memoryManager);
            }
        }
    }
}

void CollectFunction::updatePos(uint8_t* state_, ValueVector* input, uint64_t multiplicity,
    uint32_t pos, MemoryManager* memoryManager) {
    auto state = reinterpret_cast<CollectState*>(state_);
    updateSingleValue(state, input, pos, multiplicity, memoryManager);
}

void CollectFunction::initCollectStateIfNecessary(CollectState* state, MemoryManager* memoryManager,
    LogicalType& dataType) {
    if (state->factorizedTable == nullptr) {
        auto tableSchema = FactorizedTableSchema();
        tableSchema.appendColumn(ColumnSchema(false /* isUnflat */, 0 /* groupID */,
            StorageUtils::getDataTypeSize(dataType)));
        state->factorizedTable =
            std::make_unique<FactorizedTable>(memoryManager, std::move(tableSchema));
    }
}

void CollectFunction::updateSingleValue(CollectState* state, ValueVector* input, uint32_t pos,
    uint64_t multiplicity, MemoryManager* memoryManager) {
    initCollectStateIfNecessary(state, memoryManager, input->dataType);
    for (auto i = 0u; i < multiplicity; ++i) {
        auto tuple = state->factorizedTable->appendEmptyTuple();
        state->isNull = false;
        input->copyToRowData(pos, tuple, state->factorizedTable->getInMemOverflowBuffer());
    }
}

void CollectFunction::combine(uint8_t* state_, uint8_t* otherState_,
    MemoryManager* /*memoryManager*/) {
    auto otherState = reinterpret_cast<CollectState*>(otherState_);
    if (otherState->isNull) {
        return;
    }
    auto state = reinterpret_cast<CollectState*>(state_);
    if (state->isNull) {
        state->factorizedTable = std::move(otherState->factorizedTable);
        state->isNull = false;
    } else {
        state->factorizedTable->merge(*otherState->factorizedTable);
    }
    otherState->factorizedTable.reset();
}

std::unique_ptr<FunctionBindData> CollectFunction::bindFunc(const expression_vector& arguments,
    Function* definition) {
    KU_ASSERT(arguments.size() == 1);
    auto aggFuncDefinition = reinterpret_cast<AggregateFunction*>(definition);
    aggFuncDefinition->parameterTypeIDs[0] = arguments[0]->dataType.getLogicalTypeID();
    auto returnType = LogicalType::LIST(std::make_unique<LogicalType>(arguments[0]->dataType));
    return std::make_unique<FunctionBindData>(std::move(returnType));
}

function_set CollectFunction::getFunctionSet() {
    function_set result;
    for (auto isDistinct : std::vector<bool>{true, false}) {
        result.push_back(std::make_unique<AggregateFunction>(name,
            std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::LIST, initialize,
            updateAll, updatePos, combine, finalize, isDistinct, bindFunc));
    }
    return result;
}

} // namespace function
} // namespace kuzu
