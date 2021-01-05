#include "src/processor/include/operator/operator.h"

#include "src/processor/include/operator/operator_ser_deser_factory.h"

namespace graphflow {
namespace processor {

Operator::Operator(FileDeserHelper& fdsh) : prevOperator{deserializeOperator(fdsh)} {};

} // namespace processor
} // namespace graphflow