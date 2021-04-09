#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"

namespace graphflow {
namespace processor {

AdjListExtend::AdjListExtend(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseLists* lists,
    unique_ptr<PhysicalOperator> prevOperator)
    : ReadList{dataChunkPos, valueVectorPos, lists, move(prevOperator)} {
    outValueVector =
        make_shared<NodeIDVector>(((Lists<nodeID_t>*)lists)->getNodeIDCompressionScheme());
    outDataChunk = make_shared<DataChunk>(true /* initializeSelectedValuesPos */);
    outDataChunk->append(outValueVector);
    outValueVector->setDataChunkOwner(outDataChunk);
    auto listSyncState = make_shared<ListSyncState>();
    dataChunks->append(outDataChunk, listSyncState);
    handle->setListSyncState(listSyncState);
    handle->setIsAdjListHandle();
}

} // namespace processor
} // namespace graphflow
