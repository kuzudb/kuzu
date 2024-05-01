#include "processor/operator/recursive_extend/path_property_probe.h"

#include "function/hash/hash_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void PathPropertyProbe::initLocalStateInternal(ResultSet* /*resultSet_*/,
    ExecutionContext* /*context*/) {
    localState = std::make_unique<PathPropertyProbeLocalState>();
    vectors = std::make_unique<Vectors>();
    auto pathVector = resultSet->getValueVector(info->pathPos);
    auto pathNodesFieldIdx = StructType::getFieldIdx(pathVector->dataType, InternalKeyword::NODES);
    auto pathRelsFieldIdx = StructType::getFieldIdx(pathVector->dataType, InternalKeyword::RELS);
    vectors->pathNodesVector =
        StructVector::getFieldVector(pathVector.get(), pathNodesFieldIdx).get();
    vectors->pathRelsVector =
        StructVector::getFieldVector(pathVector.get(), pathRelsFieldIdx).get();
    auto pathNodesDataVector = ListVector::getDataVector(vectors->pathNodesVector);
    auto pathRelsDataVector = ListVector::getDataVector(vectors->pathRelsVector);
    auto pathNodesIDFieldIdx =
        StructType::getFieldIdx(pathNodesDataVector->dataType, InternalKeyword::ID);
    auto pathRelsIDFieldIdx =
        StructType::getFieldIdx(pathRelsDataVector->dataType, InternalKeyword::ID);
    vectors->pathNodesIDDataVector =
        StructVector::getFieldVector(pathNodesDataVector, pathNodesIDFieldIdx).get();
    vectors->pathRelsIDDataVector =
        StructVector::getFieldVector(pathRelsDataVector, pathRelsIDFieldIdx).get();
    for (auto fieldIdx : info->nodeFieldIndices) {
        vectors->pathNodesPropertyDataVectors.push_back(
            StructVector::getFieldVector(pathNodesDataVector, fieldIdx).get());
    }
    for (auto fieldIdx : info->relFieldIndices) {
        vectors->pathRelsPropertyDataVectors.push_back(
            StructVector::getFieldVector(pathRelsDataVector, fieldIdx).get());
    }
}

bool PathPropertyProbe::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto sizeProbed = 0u;
    // Scan node property
    if (sharedState->nodeHashTableState != nullptr) {
        auto nodeHashTable = sharedState->nodeHashTableState->getHashTable();
        auto nodeDataSize = ListVector::getDataVectorSize(vectors->pathNodesVector);
        while (sizeProbed < nodeDataSize) {
            auto sizeToProbe =
                std::min<uint64_t>(DEFAULT_VECTOR_CAPACITY, nodeDataSize - sizeProbed);
            probe(nodeHashTable, sizeProbed, sizeToProbe, vectors->pathNodesIDDataVector,
                vectors->pathNodesPropertyDataVectors, info->nodeTableColumnIndices);
            sizeProbed += sizeToProbe;
        }
    }
    // Scan rel property
    if (sharedState->relHashTableState != nullptr) {
        auto relHashTable = sharedState->relHashTableState->getHashTable();
        auto relDataSize = ListVector::getDataVectorSize(vectors->pathRelsVector);
        sizeProbed = 0u;
        while (sizeProbed < relDataSize) {
            auto sizeToProbe =
                std::min<uint64_t>(DEFAULT_VECTOR_CAPACITY, relDataSize - sizeProbed);
            probe(relHashTable, sizeProbed, sizeToProbe, vectors->pathRelsIDDataVector,
                vectors->pathRelsPropertyDataVectors, info->relTableColumnIndices);
            sizeProbed += sizeToProbe;
        }
    }
    return true;
}

void PathPropertyProbe::probe(kuzu::processor::JoinHashTable* hashTable, uint64_t sizeProbed,
    uint64_t sizeToProbe, ValueVector* idVector, const std::vector<ValueVector*>& propertyVectors,
    const std::vector<ft_col_idx_t>& colIndicesToScan) {
    // Hash
    for (auto i = 0u; i < sizeToProbe; ++i) {
        function::Hash::operation(idVector->getValue<internalID_t>(sizeProbed + i),
            localState->hashes[i], nullptr /* keyVector */);
    }
    // Probe hash
    for (auto i = 0u; i < sizeToProbe; ++i) {
        localState->probedTuples[i] = hashTable->getTupleForHash(localState->hashes[i]);
    }
    // Match value
    for (auto i = 0u; i < sizeToProbe; ++i) {
        while (localState->probedTuples[i]) {
            auto currentTuple = localState->probedTuples[i];
            if (*(internalID_t*)currentTuple == idVector->getValue<internalID_t>(sizeProbed + i)) {
                localState->matchedTuples[i] = currentTuple;
                break;
            }
            localState->probedTuples[i] = *hashTable->getPrevTuple(currentTuple);
        }
        KU_ASSERT(localState->matchedTuples[i] != nullptr);
    }
    // Scan table
    auto factorizedTable = hashTable->getFactorizedTable();
    for (auto i = 0u; i < sizeToProbe; ++i) {
        auto tuple = localState->matchedTuples[i];
        for (auto j = 0u; j < propertyVectors.size(); ++j) {
            auto propertyVector = propertyVectors[j];
            auto colIdx = colIndicesToScan[j];
            factorizedTable->readFlatColToFlatVector(tuple, colIdx, *propertyVector,
                sizeProbed + i);
        }
    }
}

} // namespace processor
} // namespace kuzu
