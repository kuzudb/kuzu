#include "src/processor/include/operator/physical/list_reader/adj_list_extend.h"

namespace graphflow {
namespace processor {

AdjListExtend::AdjListExtend(const uint64_t& dataChunkPos, const uint64_t& valueVectorPos,
    BaseLists* lists, shared_ptr<ListSyncer> listSyncer, unique_ptr<Operator> prevOperator)
    : ListReader{dataChunkPos, valueVectorPos, lists, listSyncer, move(prevOperator)} {
    outValueVector =
        make_shared<NodeIDVector>(((Lists<nodeID_t>*)lists)->getNodeIDCompressionScheme());
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(outValueVector);
    dataChunks->append(outDataChunk);
    handle->setIsAdjListHandle();
}

} // namespace processor
} // namespace graphflow
