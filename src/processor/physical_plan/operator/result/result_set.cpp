#include "src/processor/include/physical_plan/operator/result/result_set.h"

namespace graphflow {
namespace processor {

void ResultSet::insert(uint32_t pos, const shared_ptr<DataChunk>& dataChunk,
    const shared_ptr<ListSyncState>& listSyncState) {
    assert(dataChunks.size() > pos);
    dataChunks[pos] = dataChunk;
    listSyncStatesPerDataChunk[pos] = listSyncState;
}

uint64_t ResultSet::getNumTuples() {
    uint64_t numTuples = 1;
    for (auto& dataChunk : dataChunks) {
        numTuples *= dataChunk->state->getNumSelectedValues();
    }
    return numTuples * multiplicity;
}

unique_ptr<ResultSet> ResultSet::clone() {
    auto clonedResultSet = make_unique<ResultSet>(dataChunks.size());
    for (auto i = 0u; i < dataChunks.size(); ++i) {
        clonedResultSet->insert(i, dataChunks[i]->clone());
    }
    clonedResultSet->multiplicity = multiplicity;
    return clonedResultSet;
}

} // namespace processor
} // namespace graphflow
