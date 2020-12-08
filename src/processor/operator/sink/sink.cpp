#include "src/processor/include/operator/sink/sink.h"

namespace graphflow {
namespace processor {

void Sink::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    prevOperator->initialize(graph, morsel);
    auto prevDataChunks = prevOperator->getOutDataChunks();
}

void Sink::getNextTuples() {
    prevOperator->getNextTuples();
    uint64_t newTuples = 1;
    for (auto chunk : prevOperator->getOutDataChunks()) {
        newTuples *= chunk->size;
    }
}

} // namespace processor
} // namespace graphflow
