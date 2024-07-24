#include "planner/operator/logical_partitioner.h"

#include "binder/expression/expression_util.h"

namespace kuzu {
namespace planner {

void LogicalPartitioner::computeFactorizedSchema() {
    copyChildSchema(0);
}

void LogicalPartitioner::computeFlatSchema() {
    copyChildSchema(0);
}


} // namespace planner
} // namespace kuzu
