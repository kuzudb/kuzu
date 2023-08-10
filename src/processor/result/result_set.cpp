#include "processor/result/result_set.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

ResultSet::ResultSet(
    ResultSetDescriptor* resultSetDescriptor, storage::MemoryManager* memoryManager)
    : multiplicity{1} {
    auto numDataChunks = resultSetDescriptor->dataChunkDescriptors.size();
    dataChunks.resize(numDataChunks);
    for (auto i = 0u; i < numDataChunks; ++i) {
        auto dataChunkDescriptor = resultSetDescriptor->dataChunkDescriptors[i].get();
        auto numValueVectors = dataChunkDescriptor->logicalTypes.size();
        auto dataChunk = std::make_unique<DataChunk>(numValueVectors);
        if (dataChunkDescriptor->isSingleState) {
            dataChunk->state = DataChunkState::getSingleValueDataChunkState();
        }
        for (auto j = 0u; j < numValueVectors; ++j) {
            auto vector =
                std::make_shared<ValueVector>(dataChunkDescriptor->logicalTypes[j], memoryManager);
            dataChunk->insert(j, std::move(vector));
        }
        insert(i, std::move(dataChunk));
    }
}

uint64_t ResultSet::getNumTuplesWithoutMultiplicity(
    const std::unordered_set<uint32_t>& dataChunksPosInScope) {
    assert(!dataChunksPosInScope.empty());
    uint64_t numTuples = 1;
    for (auto& dataChunkPos : dataChunksPosInScope) {
        auto state = dataChunks[dataChunkPos]->state;
        numTuples *= state->getNumSelectedValues();
    }
    return numTuples;
}

} // namespace processor
} // namespace kuzu
