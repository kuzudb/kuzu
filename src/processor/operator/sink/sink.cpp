#include "src/processor/include/operator/sink/sink.h"

namespace graphflow {
namespace processor {

void Sink::getNextTuples() {
    prevOperator->getNextTuples();
    numTuples += prevOperator->getOutDataChunks()->getNumTuples();
}

} // namespace processor
} // namespace graphflow
