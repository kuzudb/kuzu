#include "processor/operator/recursive_extend/recursive_join_property_probe.h"

#include "function/hash/vector_hash_operations.h"
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void RecursiveJoinPropertyProbe::initLocalStateInternal(
    ResultSet* resultSet_, ExecutionContext* context) {
    localState = std::make_unique<RecursiveJoinPropertyProbeLocalState>();
    auto pathVector = resultSet->getValueVector(pathPos);
    auto pathNodesFieldIdx = common::StructType::getFieldIdx(
        &pathVector->dataType, common::InternalKeyword::NODES);
    auto pathNodesVector = StructVector::getFieldVector(pathVector.get(), pathNodesFieldIdx);
    auto pathNodesDataVector = ListVector::getDataVector(pathNodesVector.get());
    assert(pathNodesDataVector->dataType.getPhysicalType() == common::PhysicalTypeID::STRUCT);
    auto pathNodesFieldTypes = StructType::getFieldIdx(&pathNodesDataVector->dataType, InternalKeyword::ID);
//    assert()
    auto pathNodesIDVector = StructVector::getFieldVector(pathNodesDataVector, );
//    for (auto i = 1u; i < StructType::get)
}

bool RecursiveJoinPropertyProbe::getNextTuplesInternal(ExecutionContext* context) {
    if (children[0]->getNextTuple(context)) {
        return false;
    }
    auto hashTable = sharedState->getHashTable();
    auto dataSize = ListVector::getDataVectorSize(pathNodeIDVector);
    auto dataVector = ListVector::getDataVector(pathNodeIDVector);
    auto sizeProbed = 0u;
    // TODO: wrap as function
    while (sizeProbed < dataSize) {
        auto sizeToProbe = std::min<uint64_t>(DEFAULT_VECTOR_CAPACITY, dataSize - sizeProbed);
        // Hash
        for (auto i = 0u; i < sizeToProbe; ++i) {
            function::operation::Hash::operation(
                dataVector->getValue<internalID_t>(sizeProbed + i), localState->hashes[i]);
        }
        // Probe hash
        for (auto i = 0u; i < sizeToProbe; ++i) {
            localState->probedTuples[i] = hashTable->getTupleForHash(localState->hashes[i]);
        }
        // Match value
        for (auto i = 0u; i < sizeProbed; ++i) {
            while (localState->probedTuples[i]) {
                auto currentTuple = localState->probedTuples[i];
                if (*(nodeID_t*)currentTuple == dataVector->getValue<nodeID_t>(sizeProbed + 1)) {
                    localState->matchedTuples[i] = currentTuple;
                    break;
                }
                localState->probedTuples[i] = *hashTable->getPrevTuple(currentTuple);
            }
            assert(localState->matchedTuples[i] != nullptr);
        }
        // Scan table
        auto factorizedTable = hashTable->getFactorizedTable();
        for (auto i = 0u; i < sizeProbed; ++i) {
            auto tuple = localState->matchedTuples[i];
            for (auto j = 0u; j < propertyVectors.size(); ++j) {
                auto propertyVector = propertyVectors[i];
                factorizedTable->readFlatColToFlatVector(
                    tuple, j + 1, *propertyVector, sizeProbed + 1);
            }
        }
        sizeProbed += sizeToProbe;
    }
    return true;
}

} // namespace processor
} // namespace kuzu