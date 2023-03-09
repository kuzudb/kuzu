#include "processor/operator/var_length_extend/var_length_extend.h"

namespace kuzu {
namespace processor {

void VarLengthExtend::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    boundNodeValueVector = resultSet->getValueVector(boundNodeDataPos).get();
    nbrNodeValueVector = resultSet->getValueVector(nbrNodeDataPos).get();
}

} // namespace processor
} // namespace kuzu
