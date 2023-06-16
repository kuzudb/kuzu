#include "processor/operator/recursive_extend/recursive_join_property_probe.h"

#include "function/hash/vector_hash_operations.h"
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void RecursiveJoinPropertyProbe::initLocalStateInternal(
    ResultSet* resultSet_, ExecutionContext* context) {
    localState = std::make_unique<RecursiveJoinPropertyProbeLocalState>();
    auto pathVector = resultSet->getValueVector(pathPos);
    auto pathNodesFieldIdx =
        common::StructType::getFieldIdx(&pathVector->dataType, common::InternalKeyword::NODES);
    pathNodesVector = StructVector::getFieldVector(pathVector.get(), pathNodesFieldIdx).get();
    auto pathNodesDataVector = ListVector::getDataVector(pathNodesVector);
    auto numFields = StructType::getNumFields(&pathNodesDataVector->dataType);
    pathNodesIDDataVector = StructVector::getFieldVector(pathNodesDataVector, 0).get();
    assert(
        pathNodesIDDataVector->dataType.getPhysicalType() == common::PhysicalTypeID::INTERNAL_ID);
    for (auto i = 1u; i < numFields; ++i) {
        pathNodesPropertyDataVectors.push_back(
            StructVector::getFieldVector(pathNodesDataVector, i).get());
    }
}

bool RecursiveJoinPropertyProbe::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto hashTable = sharedState->getHashTable();
    auto dataSize = ListVector::getDataVectorSize(pathNodesVector);
    auto sizeProbed = 0u;
    // TODO: wrap as function
    while (sizeProbed < dataSize) {
        auto sizeToProbe = std::min<uint64_t>(DEFAULT_VECTOR_CAPACITY, dataSize - sizeProbed);
        // Hash
        for (auto i = 0u; i < sizeToProbe; ++i) {
            function::operation::Hash::operation(
                pathNodesIDDataVector->getValue<internalID_t>(sizeProbed + i),
                localState->hashes[i]);
        }
        // Probe hash
        for (auto i = 0u; i < sizeToProbe; ++i) {
            localState->probedTuples[i] = hashTable->getTupleForHash(localState->hashes[i]);
        }
        // Match value
        for (auto i = 0u; i < sizeToProbe; ++i) {
            while (localState->probedTuples[i]) {
                auto currentTuple = localState->probedTuples[i];
                if (*(nodeID_t*)currentTuple ==
                    pathNodesIDDataVector->getValue<nodeID_t>(sizeProbed + i)) {
                    localState->matchedTuples[i] = currentTuple;
                    break;
                }
                localState->probedTuples[i] = *hashTable->getPrevTuple(currentTuple);
            }
            assert(localState->matchedTuples[i] != nullptr);
        }
        // Scan table
        auto factorizedTable = hashTable->getFactorizedTable();
        for (auto i = 0u; i < sizeToProbe; ++i) {
            auto tuple = localState->matchedTuples[i];
            for (auto j = 0u; j < pathNodesPropertyDataVectors.size(); ++j) {
                auto propertyVector = pathNodesPropertyDataVectors[j];
                auto colIdx = colIndicesToScan[j];
                factorizedTable->readFlatColToFlatVector(
                    tuple, colIdx, *propertyVector, sizeProbed + i);
            }
        }
        sizeProbed += sizeToProbe;
    }
    return true;
}

} // namespace processor
} // namespace kuzu