#include "function/schema/vector_node_rel_functions.h"

#include "common/vector/value_vector.h"
#include "function/hash/hash_functions.h"
#include "function/scalar_function.h"
#include "function/schema/offset_functions.h"
#include "function/unary_function_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
    void* /*dataPtr*/ = nullptr) {
    KU_ASSERT(params.size() == 1);
    UnaryFunctionExecutor::execute<internalID_t, int64_t, Offset>(*params[0], result);
}

function_set OffsetFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERNAL_ID}, LogicalTypeID::INT64, execFunc));
    return functionSet;
}

enum DistinctType { InternalID = 0, NodeInRecursive = 1, RelInRecursive = 2 };

static std::string DistinctTypeToString(DistinctType type) {
    std::string str;
    switch (type) {
    case InternalID:
        return "InternalID";
    case NodeInRecursive:
        return "NodeInRecursive";
    case RelInRecursive:
        return "RelInRecursive";
    default:
        return "Unknown";
    }
}

static std::vector<internalID_t> getData(const std::vector<std::shared_ptr<ValueVector>>& params,
    uint64_t index, uint64_t paramPos, DistinctType distinctType) {
    if (params[index]->dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID) {
        auto paramData = ((internalID_t*)params[index]->getData())[paramPos];
        return std::vector<internalID_t>{paramData};
    } else if (params[index]->dataType.getLogicalTypeID() == LogicalTypeID::RECURSIVE_REL) {
        auto fieldIdx = -1, internalIDFieldIdx = -1;
        if (distinctType == DistinctType::NodeInRecursive) {
            fieldIdx = 0;
            KU_ASSERT(fieldIdx == common::StructType::getFieldIdx(params[index]->dataType,
                                      common::InternalKeyword::NODES));
            internalIDFieldIdx = 0;
        } else if (distinctType == DistinctType::RelInRecursive) {
            fieldIdx = 1;
            KU_ASSERT(fieldIdx == common::StructType::getFieldIdx(params[index]->dataType,
                                      common::InternalKeyword::RELS));
            internalIDFieldIdx = 3;
        } else {
            throw RuntimeException("Error Distinct Function with input RECURSIVE_REL: " +
                                   DistinctTypeToString(distinctType));
        }
        auto listVector = common::StructVector::getFieldVector(params[index].get(), fieldIdx).get();
        auto listDataVector = common::ListVector::getDataVector(listVector);
        KU_ASSERT(internalIDFieldIdx == common::StructType::getFieldIdx(listDataVector->dataType,
                                            common::InternalKeyword::ID));
        auto internalIDsVector =
            common::StructVector::getFieldVector(listDataVector, internalIDFieldIdx).get();
        auto& listEntry = listVector->getValue<common::list_entry_t>(paramPos);
        std::vector<internalID_t> ids;
        ids.reserve(listEntry.size);
        for (auto i = 0u; i < listEntry.size; ++i) {
            auto internalID =
                internalIDsVector->getValue<common::internalID_t>(listEntry.offset + i);
            ids.push_back(internalID);
        }
        return ids;
    } else {
        throw RuntimeException(
            "isDistinct data type error, type is" + params[index]->dataType.toString());
    }
}

static uint8_t isInternalIDDistinct(const std::vector<std::shared_ptr<ValueVector>>& params,
    sel_t unFlatPos, std::unordered_set<common::nodeID_t, InternalIDHasher>& internalIDSet,
    DistinctType distinctType) {
    internalIDSet.clear();
    uint8_t distinct = true;
    for (auto paramIdx = 0u; paramIdx < params.size(); ++paramIdx) {
        auto paramPos = params[paramIdx]->state->isFlat() ?
                            params[paramIdx]->state->getSelVector()[0] :
                            unFlatPos;
        auto paramData = getData(params, paramIdx, paramPos, distinctType);
        for (auto& internalID : paramData) {
            if (internalIDSet.contains(internalID)) {
                return false;
            }
            internalIDSet.insert(internalID);
        }
    }
    return distinct;
}

static void distinctExecFuncWithType(const std::vector<std::shared_ptr<ValueVector>>& params,
    ValueVector& result, DistinctType distinctType) {
    result.resetAuxiliaryBuffer();
    std::unordered_set<nodeID_t, InternalIDHasher> internalIDSet;
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
        ++selectedPos) {
        auto pos = result.state->getSelVector()[selectedPos];
        ((uint8_t*)result.getData())[pos] =
            isInternalIDDistinct(params, pos, internalIDSet, distinctType);
    }
}
static void IsIDDistinctExecFunc(const std::vector<std::shared_ptr<ValueVector>>& params,
    ValueVector& result, void* /*dataPtr*/ = nullptr) {
    distinctExecFuncWithType(params, result, DistinctType::InternalID);
}

static void IsNodeDistinctExecFunc(const std::vector<std::shared_ptr<ValueVector>>& params,
    ValueVector& result, void* /*dataPtr*/ = nullptr) {
    distinctExecFuncWithType(params, result, DistinctType::NodeInRecursive);
}

static void IsRelDistinctExecFunc(const std::vector<std::shared_ptr<ValueVector>>& params,
    ValueVector& result, void* /*dataPtr*/ = nullptr) {
    distinctExecFuncWithType(params, result, DistinctType::RelInRecursive);
}

static bool distinctSelectFuncWithType(
    const std::vector<std::shared_ptr<common::ValueVector>>& params, SelectionVector& selVector,
    DistinctType distinctType) {

    bool hasUnFlat = false;
    auto firstUnFlatIndex = 0u;
    for (auto i = 0u; i < params.size(); ++i) {
        if (!params[i]->state->isFlat())  {
            hasUnFlat = true;
            firstUnFlatIndex = i;
        }
    }
    std::unordered_set<nodeID_t, InternalIDHasher> internalIDSet;

    if (!hasUnFlat) {
        for (auto paramIdx = 0u; paramIdx< params.size();++paramIdx) {
            auto paramPos = params[paramIdx]->state->getSelVector()[0];
            auto paramData = getData(params, paramIdx, paramPos, distinctType);
            for (auto internalID : paramData) {
                if (internalIDSet.contains(internalID)) {
                    return false;
                }
                internalIDSet.insert(internalID);
            }
        }
        return true;
    } else {
        auto numSelectedValues = 0u;
        auto buffer = selVector.getMutableBuffer();
        auto& firstUnFlatSelectionVector = params[firstUnFlatIndex]->state->getSelVector();
        if (firstUnFlatSelectionVector.isUnfiltered()) {
            for (auto i = 0u; i < firstUnFlatSelectionVector.getSelSize(); ++i) {
                auto pos = i;
                uint8_t isDistinct = isInternalIDDistinct(params, pos, internalIDSet, distinctType);
                buffer[numSelectedValues] = i;
                numSelectedValues += isDistinct;
            }
        } else {
            for (auto i = 0u; i < firstUnFlatSelectionVector.getSelSize(); ++i) {
                auto pos = firstUnFlatSelectionVector[i];
                uint8_t isDistinct = isInternalIDDistinct(params, pos, internalIDSet, distinctType);
                buffer[numSelectedValues] = pos;
                numSelectedValues += isDistinct;
            }
        }
        selVector.setSelSize(numSelectedValues);
        return numSelectedValues > 0;
    }
}
static bool IsIDDistinctSelectFunc(const std::vector<std::shared_ptr<common::ValueVector>>& params,
    common::SelectionVector& selVector) {
    return distinctSelectFuncWithType(params, selVector, DistinctType::InternalID);
}

static bool IsNodeDistinctSelectFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& params,
    common::SelectionVector& selVector) {
    return distinctSelectFuncWithType(params, selVector, DistinctType::NodeInRecursive);
}

static bool IsRelDistinctSelectFunc(const std::vector<std::shared_ptr<common::ValueVector>>& params,
    common::SelectionVector& selVector) {
    return distinctSelectFuncWithType(params, selVector, DistinctType::RelInRecursive);
}


function_set IsIDDistinctFunction::getFunctionSet() {
    function_set functionSet;
    auto function =
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::INTERNAL_ID},
            common::LogicalTypeID::BOOL, IsIDDistinctExecFunc, IsIDDistinctSelectFunc);
    function->isVarLength = true;
    functionSet.push_back(std::move(function));

    return functionSet;
}

function_set IsNodeDistinctFunction::getFunctionSet() {
    function_set functionSet;
    auto function =
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
            common::LogicalTypeID::BOOL, IsNodeDistinctExecFunc, IsNodeDistinctSelectFunc);
    function->isVarLength = true;
    functionSet.push_back(std::move(function));

    return functionSet;
}

function_set IsRelDistinctFunction::getFunctionSet() {
    function_set functionSet;
    auto function =
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
            common::LogicalTypeID::BOOL, IsRelDistinctExecFunc, IsRelDistinctSelectFunc);
    function->isVarLength = true;
    functionSet.push_back(std::move(function));

    return functionSet;
}
} // namespace function
} // namespace kuzu
