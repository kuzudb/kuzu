#include "processor/operator/var_length_extend/var_length_extend.h"

namespace kuzu {
namespace processor {

VarLengthExtend::VarLengthExtend(const DataPos& boundNodeDataPos, const DataPos& nbrNodeDataPos,
    BaseColumnOrList* storage, uint8_t lowerBound, uint8_t upperBound,
    unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
    : PhysicalOperator{move(child), id, paramsString}, boundNodeDataPos{boundNodeDataPos},
      nbrNodeDataPos{nbrNodeDataPos}, storage{storage}, lowerBound{lowerBound}, upperBound{
                                                                                    upperBound} {
    dfsLevelInfos.resize(upperBound);
}

void VarLengthExtend::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    boundNodeValueVector = resultSet->getValueVector(boundNodeDataPos);
    nbrNodeValueVector = resultSet->getValueVector(nbrNodeDataPos);
}

} // namespace processor
} // namespace kuzu
