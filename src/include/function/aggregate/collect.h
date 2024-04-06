#pragma once

#include "function/aggregate_function.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace function {

struct CollectFunction {
    static constexpr const char* name = "COLLECT";

    struct CollectState : public AggregateState {
        CollectState() : factorizedTable{nullptr} {}
        inline uint32_t getStateSize() const override { return sizeof(*this); }
        void moveResultToVector(common::ValueVector* outputVector, uint64_t pos) override {
            auto listEntry =
                common::ListVector::addList(outputVector, factorizedTable->getNumTuples());
            outputVector->setValue<common::list_entry_t>(pos, listEntry);
            auto outputDataVector = common::ListVector::getDataVector(outputVector);
            for (auto i = 0u; i < listEntry.size; i++) {
                outputDataVector->copyFromRowData(listEntry.offset + i,
                    factorizedTable->getTuple(i));
            }
            // CollectStates are stored in factorizedTable entries. When the factorizedTable is
            // destructed, the destructor of CollectStates won't be called. Therefore, we need to
            // manually deallocate the memory of CollectStates.
            factorizedTable.reset();
        }

        std::unique_ptr<processor::FactorizedTable> factorizedTable;
    };

    static std::unique_ptr<AggregateState> initialize();

    static void updateAll(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        storage::MemoryManager* memoryManager);

    static void updatePos(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        uint32_t pos, storage::MemoryManager* memoryManager);

    static void initCollectStateIfNecessary(CollectState* state,
        storage::MemoryManager* memoryManager, common::LogicalType& dataType);

    static void updateSingleValue(CollectState* state, common::ValueVector* input, uint32_t pos,
        uint64_t multiplicity, storage::MemoryManager* memoryManager);

    static void combine(uint8_t* state_, uint8_t* otherState_,
        storage::MemoryManager* /*memoryManager*/);

    static void finalize(uint8_t* /*state_*/) {}

    static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
        Function* definition);

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
