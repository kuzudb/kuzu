#include "src/processor/include/operator/physical/sink/sink.h"

namespace graphflow {
namespace processor {

void Sink::getNextTuples() {
    prevOperator->getNextTuples();
    numTuples += prevOperator->getDataChunks()->getNumTuples();
}

} // namespace processor
} // namespace graphflow
