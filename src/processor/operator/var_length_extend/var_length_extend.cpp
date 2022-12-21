#include "processor/operator/var_length_extend/var_length_extend.h"

namespace kuzu {
namespace processor {

void VarLengthExtend::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    boundNodeValueVector = resultSet->getValueVector(boundNodeDataPos);
    nbrNodeValueVector = resultSet->getValueVector(nbrNodeDataPos);
}

} // namespace processor
} // namespace kuzu
