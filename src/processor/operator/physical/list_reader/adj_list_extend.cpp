#include "src/processor/include/operator/physical/list_reader/adj_list_extend.h"

namespace graphflow {
namespace processor {

AdjListExtend::AdjListExtend(const uint64_t& inDataChunkIdx, const uint64_t& inValueVectorIdx,
    BaseLists* lists, unique_ptr<Operator> prevOperator)
    : ListReader{inDataChunkIdx, inValueVectorIdx, lists, move(prevOperator)} {
    inDataChunk->setAsFlat();
    outNodeIDVector =
        make_shared<NodeIDVector>(((Lists<nodeID_t>*)lists)->getNodeIDCompressionScheme());
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(outNodeIDVector);
    dataChunks->append(outDataChunk);
}

} // namespace processor
} // namespace graphflow
