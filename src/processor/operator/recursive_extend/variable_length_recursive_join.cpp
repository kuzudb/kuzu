#include "processor/operator/recursive_extend/variable_length_recursive_join.h"

namespace kuzu {
namespace processor {

void VariableLengthRecursiveJoin::initLocalStateInternal(
    ResultSet* resultSet_, ExecutionContext* context) {
    BaseRecursiveJoin::initLocalStateInternal(resultSet_, context);
    auto maxNodeOffset = nodeTable->getMaxNodeOffset(transaction);
    bfsMorsel = std::make_unique<VariableLengthBFSMorsel>(
        maxNodeOffset, lowerBound, upperBound, sharedState->semiMask.get());
    pathScanner = std::make_unique<PathScanner>(*bfsMorsel, lowerBound, upperBound);
}

} // namespace processor
} // namespace kuzu
