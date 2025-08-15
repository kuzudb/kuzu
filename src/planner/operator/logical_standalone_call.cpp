#include "planner/operator/logical_standalone_call.h"

#include "main/db_config.h"

namespace kuzu {
namespace planner {

std::string LogicalStandaloneCall::getExpressionsForPrinting() const {
    return option->name;
}

} // namespace planner
} // namespace kuzu
