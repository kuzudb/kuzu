#include "src/processor/include/operator/physical/list_reader/adj_list_extend.h"

namespace graphflow {
namespace processor {

AdjListExtend::AdjListExtend(const uint64_t& dataChunkPos, const uint64_t& valueVectorPos,
    BaseLists* lists, unique_ptr<Operator> prevOperator)
    : ListReader{dataChunkPos, valueVectorPos, lists, move(prevOperator)} {
    inDataChunk->setAsFlat();
    outNodeIDVector =
        make_shared<NodeIDVector>(((Lists<nodeID_t>*)lists)->getNodeIDCompressionScheme());
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(outNodeIDVector);
    dataChunks->append(outDataChunk);
}

} // namespace processor
} // namespace graphflow
