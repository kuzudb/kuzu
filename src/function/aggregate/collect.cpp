#include "function/aggregate_function.h"
#include "processor/result/factorized_table.h"
#include "storage/storage_utils.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

struct CollectState : public AggregateState {
    CollectState() : factorizedTable{nullptr} {}
    uint32_t getStateSize() const override { return sizeof(*this); }
    void moveResultToVector(common::ValueVector* outputVector, uint64_t pos) override;

    std::unique_ptr<processor::FactorizedTable> factorizedTable;
};

void CollectState::moveResultToVector(common::ValueVector* outputVector, uint64_t pos) {
    auto listEntry = common::ListVector::addList(outputVector, factorizedTable->getNumTuples());
    outputVector->setValue<common::list_entry_t>(pos, listEntry);
    auto outputDataVector = common::ListVector::getDataVector(outputVector);
    for (auto i = 0u; i < listEntry.size; i++) {
        outputDataVector->copyFromRowData(listEntry.offset + i, factorizedTable->getTuple(i));
    }
    // CollectStates are stored in factorizedTable entries. When the factorizedTable is
    // destructed, the destructor of CollectStates won't be called. Therefore, we need to
    // manually deallocate the memory of CollectStates.
    factorizedTable.reset();
}

static std::unique_ptr<AggregateState> initialize() {
    return std::make_unique<CollectState>();
}

static void initCollectStateIfNecessary(CollectState* state, MemoryManager* memoryManager,
    LogicalType& dataType) {
    if (state->factorizedTable == nullptr) {
        auto tableSchema = FactorizedTableSchema();
        tableSchema.appendColumn(ColumnSchema(false /* isUnflat */, 0 /* groupID */,
            StorageUtils::getDataTypeSize(dataType)));
        state->factorizedTable =
            std::make_unique<FactorizedTable>(memoryManager, std::move(tableSchema));
    }
}

static void updateSingleValue(CollectState* state, ValueVector* input, uint32_t pos,
    uint64_t multiplicity, MemoryManager* memoryManager) {
    initCollectStateIfNecessary(state, memoryManager, input->dataType);
    for (auto i = 0u; i < multiplicity; ++i) {
        auto tuple = state->factorizedTable->appendEmptyTuple();
        state->isNull = false;
        input->copyToRowData(pos, tuple, state->factorizedTable->getInMemOverflowBuffer());
    }
}

static void updateAll(uint8_t* state_, ValueVector* input, uint64_t multiplicity,
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

static void updatePos(uint8_t* state_, ValueVector* input, uint64_t multiplicity, uint32_t pos,
    MemoryManager* memoryManager) {
    auto state = reinterpret_cast<CollectState*>(state_);
    updateSingleValue(state, input, pos, multiplicity, memoryManager);
}

static void finalize(uint8_t* /*state_*/) {}

static void combine(uint8_t* state_, uint8_t* otherState_, MemoryManager* /*memoryManager*/) {
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

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    KU_ASSERT(input.arguments.size() == 1);
    auto aggFuncDefinition = reinterpret_cast<AggregateFunction*>(input.definition);
    aggFuncDefinition->parameterTypeIDs[0] = input.arguments[0]->dataType.getLogicalTypeID();
    auto returnType = LogicalType::LIST(input.arguments[0]->dataType.copy());
    return std::make_unique<FunctionBindData>(std::move(returnType));
}

function_set CollectFunction::getFunctionSet() {
    function_set result;
    for (auto isDistinct : std::vector<bool>{true, false}) {
        result.push_back(std::make_unique<AggregateFunction>(name,
            std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::LIST, initialize,
            updateAll, updatePos, combine, finalize, isDistinct, bindFunc,
            nullptr /* paramRewriteFunc */));
    }
    return result;
}

} // namespace function
} // namespace kuzu
