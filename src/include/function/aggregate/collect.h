#pragma once

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/vector/value_vector_utils.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace function {

struct CollectFunction {

    struct CollectState : public AggregateState {
        CollectState() : factorizedTable{nullptr} {}
        inline uint32_t getStateSize() const override { return sizeof(*this); }
        void moveResultToVector(common::ValueVector* outputVector, uint64_t pos) override {
            auto dstKUList = outputVector->getValue<common::ku_list_t>(pos);
            auto numBytesPerElement =
                factorizedTable->getTableSchema()->getColumn(0)->getNumBytes();
            dstKUList.size = factorizedTable->getNumTuples();
            dstKUList.overflowPtr =
                reinterpret_cast<uint64_t>(outputVector->getOverflowBuffer().allocateSpace(
                    factorizedTable->getNumTuples() * numBytesPerElement));
            outputVector->setValue<common::ku_list_t>(pos, dstKUList);
            switch (outputVector->dataType.childType->typeID) {
            case common::STRING: {
                for (auto i = 0u; i < dstKUList.size; i++) {
                    common::InMemOverflowBufferUtils::copyString(
                        *reinterpret_cast<common::ku_string_t*>(factorizedTable->getTuple(i)),
                        reinterpret_cast<common::ku_string_t*>(dstKUList.overflowPtr)[i],
                        outputVector->getOverflowBuffer());
                }
            } break;
            case common::VAR_LIST: {
                for (auto i = 0u; i < dstKUList.size; i++) {
                    common::InMemOverflowBufferUtils::copyListRecursiveIfNested(
                        *reinterpret_cast<common::ku_list_t*>(factorizedTable->getTuple(i)),
                        reinterpret_cast<common::ku_list_t*>(dstKUList.overflowPtr)[i],
                        *outputVector->dataType.childType, outputVector->getOverflowBuffer());
                }
            } break;
            default: {
                for (auto i = 0u; i < dstKUList.size; i++) {
                    memcpy(
                        reinterpret_cast<uint8_t*>(dstKUList.overflowPtr) + i * numBytesPerElement,
                        factorizedTable->getTuple(i), numBytesPerElement);
                }
            }
            }
            // CollectStates are stored in factorizedTable entries. When the factorizedTable is
            // destructed, the destructor of CollectStates won't be called. Therefore, we need to
            // manually deallocate the memory of CollectStates.
            factorizedTable.reset();
        }

        std::unique_ptr<processor::FactorizedTable> factorizedTable;
    };

    static std::unique_ptr<AggregateState> initialize() { return std::make_unique<CollectState>(); }

    static void updateAll(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        storage::MemoryManager* memoryManager) {
        assert(!input->state->isFlat());
        auto state = reinterpret_cast<CollectState*>(state_);
        if (input->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < input->state->selVector->selectedSize; ++i) {
                auto pos = input->state->selVector->selectedPositions[i];
                updateSingleValue(state, input, pos, multiplicity, memoryManager);
            }
        } else {
            for (auto i = 0u; i < input->state->selVector->selectedSize; ++i) {
                auto pos = input->state->selVector->selectedPositions[i];
                if (!input->isNull(pos)) {
                    updateSingleValue(state, input, pos, multiplicity, memoryManager);
                }
            }
        }
    }

    static inline void updatePos(uint8_t* state_, common::ValueVector* input, uint64_t multiplicity,
        uint32_t pos, storage::MemoryManager* memoryManager) {
        auto state = reinterpret_cast<CollectState*>(state_);
        updateSingleValue(state, input, pos, multiplicity, memoryManager);
    }

    static void initCollectStateIfNecessary(
        CollectState* state, storage::MemoryManager* memoryManager, common::DataType& dataType) {
        if (state->factorizedTable == nullptr) {
            auto tableSchema = std::make_unique<processor::FactorizedTableSchema>();
            tableSchema->appendColumn(
                std::make_unique<processor::ColumnSchema>(false /* isUnflat */,
                    0 /* dataChunkPos */, common::Types::getDataTypeSize(dataType)));
            state->factorizedTable =
                std::make_unique<processor::FactorizedTable>(memoryManager, std::move(tableSchema));
        }
    }

    static void updateSingleValue(CollectState* state, common::ValueVector* input, uint32_t pos,
        uint64_t multiplicity, storage::MemoryManager* memoryManager) {
        initCollectStateIfNecessary(state, memoryManager, input->dataType);
        for (auto i = 0u; i < multiplicity; ++i) {
            auto tuple = state->factorizedTable->appendEmptyTuple();
            state->isNull = false;
            common::ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
                *input, pos, tuple, *state->factorizedTable->getInMemOverflowBuffer());
        }
    }

    static void combine(
        uint8_t* state_, uint8_t* otherState_, storage::MemoryManager* memoryManager) {
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
    }

    static void finalize(uint8_t* state_) {}

    static void bindFunc(const std::vector<common::DataType>& argumentTypes,
        FunctionDefinition* definition, common::DataType& returnType) {
        assert(argumentTypes.size() == 1);
        auto aggFuncDefinition = reinterpret_cast<AggregateFunctionDefinition*>(definition);
        aggFuncDefinition->aggregateFunction->setInputDataType(argumentTypes[0]);
        returnType = common::DataType(
            common::VAR_LIST, std::make_unique<common::DataType>(argumentTypes[0]));
    }
};

} // namespace function
} // namespace kuzu
