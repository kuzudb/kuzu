#include "src/processor/include/operator/sink/sink.h"

#include "src/processor/include/operator/operator_ser_deser_factory.h"

namespace graphflow {
namespace processor {

Sink::Sink(FileDeserHelper& fdsh) {
    this->setPrevOperator(deserializeOperator(fdsh));
}

void Sink::getNextTuples() {
    prevOperator->getNextTuples();
    numTuples += prevOperator->getOutDataChunks()->getNumTuples();
}

void Sink::serialize(FileSerHelper& fsh) {
    string typeIDStr = typeid(Sink).name();
    fsh.writeString(typeIDStr);
    Operator::serialize(fsh);
}

} // namespace processor
} // namespace graphflow
