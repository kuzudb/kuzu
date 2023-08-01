#pragma once

#include <functional>

#include "common/vector/value_vector.h"
#include "function/hash/hash_functions.h"

namespace kuzu {
namespace function {

static bool isAllInternalIDDistinct(common::ValueVector* dataVector, common::offset_t startOffset,
    uint64_t size, std::unordered_set<common::internalID_t, InternalIDHasher>& internalIDSet) {
    internalIDSet.clear();
    for (auto i = 0; i < size; ++i) {
        auto& internalID = dataVector->getValue<common::internalID_t>(startOffset + i);
        if (internalIDSet.contains(internalID)) {
            return false;
        }
        internalIDSet.insert(internalID);
    }
    return true;
}

// Note: this executor is only used for isTrail and isAcyclic. So we add some ad-hoc optimization
// into executor, e.g. internalIDSet. A more general implementation can be done once needed. But
// pay attention to the performance drop. Depends on how bad it becomes, we may want to implement
// customized executors.
struct UnaryPathExecutor {
    static void executeNodeIDs(common::ValueVector& input, common::ValueVector& result) {
        auto nodesFieldIdx = 0;
        assert(nodesFieldIdx ==
               common::StructType::getFieldIdx(&input.dataType, common::InternalKeyword::NODES));
        auto nodesVector = common::StructVector::getFieldVector(&input, nodesFieldIdx).get();
        auto internalIDFieldIdx = 0;
        execute(*input.state->selVector, *nodesVector, internalIDFieldIdx, result);
    }

    static void executeRelIDs(common::ValueVector& input, common::ValueVector& result) {
        auto relsFieldIdx = 1;
        assert(relsFieldIdx ==
               common::StructType::getFieldIdx(&input.dataType, common::InternalKeyword::RELS));
        auto relsVector = common::StructVector::getFieldVector(&input, relsFieldIdx).get();
        auto internalIDFieldIdx = 3;
        execute(*input.state->selVector, *relsVector, internalIDFieldIdx, result);
    }

    static bool selectNodeIDs(
        common::ValueVector& input, common::SelectionVector& selectionVector) {
        auto nodesFieldIdx = 0;
        assert(nodesFieldIdx ==
               common::StructType::getFieldIdx(&input.dataType, common::InternalKeyword::NODES));
        auto nodesVector = common::StructVector::getFieldVector(&input, nodesFieldIdx).get();
        auto internalIDFieldIdx = 0;
        return select(*input.state->selVector, *nodesVector, internalIDFieldIdx, selectionVector);
    }

    static bool selectRelIDs(common::ValueVector& input, common::SelectionVector& selectionVector) {
        auto relsFieldIdx = 1;
        assert(relsFieldIdx ==
               common::StructType::getFieldIdx(&input.dataType, common::InternalKeyword::RELS));
        auto relsVector = common::StructVector::getFieldVector(&input, relsFieldIdx).get();
        auto internalIDFieldIdx = 3;
        return select(*input.state->selVector, *relsVector, internalIDFieldIdx, selectionVector);
    }

private:
    static void execute(const common::SelectionVector& inputSelVector,
        common::ValueVector& listVector, common::struct_field_idx_t fieldIdx,
        common::ValueVector& result) {
        auto listDataVector = common::ListVector::getDataVector(&listVector);
        assert(fieldIdx == common::StructType::getFieldIdx(
                               &listDataVector->dataType, common::InternalKeyword::ID));
        auto internalIDsVector =
            common::StructVector::getFieldVector(listDataVector, fieldIdx).get();
        std::unordered_set<common::nodeID_t, InternalIDHasher> internalIDSet;
        if (inputSelVector.isUnfiltered()) {
            for (auto i = 0; i < inputSelVector.selectedSize; ++i) {
                auto& listEntry = listVector.getValue<common::list_entry_t>(i);
                bool isTrail = isAllInternalIDDistinct(
                    internalIDsVector, listEntry.offset, listEntry.size, internalIDSet);
                result.setValue<bool>(i, isTrail);
            }
        } else {
            for (auto i = 0; i < inputSelVector.selectedSize; ++i) {
                auto pos = inputSelVector.selectedPositions[i];
                auto& listEntry = listVector.getValue<common::list_entry_t>(pos);
                bool isTrail = isAllInternalIDDistinct(
                    internalIDsVector, listEntry.offset, listEntry.size, internalIDSet);
                result.setValue<bool>(pos, isTrail);
            }
        }
    }

    static bool select(const common::SelectionVector& inputSelVector,
        common::ValueVector& listVector, common::struct_field_idx_t fieldIdx,
        common::SelectionVector& selectionVector) {
        auto listDataVector = common::ListVector::getDataVector(&listVector);
        assert(fieldIdx == common::StructType::getFieldIdx(
                               &listDataVector->dataType, common::InternalKeyword::ID));
        auto internalIDsVector =
            common::StructVector::getFieldVector(listDataVector, fieldIdx).get();
        std::unordered_set<common::nodeID_t, InternalIDHasher> internalIDSet;
        auto numSelectedValues = 0u;
        auto buffer = selectionVector.getSelectedPositionsBuffer();
        if (inputSelVector.isUnfiltered()) {
            for (auto i = 0; i < inputSelVector.selectedSize; ++i) {
                auto& listEntry = listVector.getValue<common::list_entry_t>(i);
                bool isTrail = isAllInternalIDDistinct(
                    internalIDsVector, listEntry.offset, listEntry.size, internalIDSet);
                buffer[numSelectedValues] = i;
                numSelectedValues += isTrail;
            }
        } else {
            for (auto i = 0; i < inputSelVector.selectedSize; ++i) {
                auto pos = inputSelVector.selectedPositions[i];
                auto& listEntry = listVector.getValue<common::list_entry_t>(pos);
                bool isTrail = isAllInternalIDDistinct(
                    internalIDsVector, listEntry.offset, listEntry.size, internalIDSet);
                buffer[numSelectedValues] = pos;
                numSelectedValues += isTrail;
            }
        }
        selectionVector.selectedSize = numSelectedValues;
        return numSelectedValues > 0;
    }
};

} // namespace function
} // namespace kuzu
