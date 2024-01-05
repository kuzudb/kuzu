#include "binder/bound_extension_statement.h"
#include "common/cast.h"
#include "planner/operator/logical_extension.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::planExtension(const BoundStatement& statement) {
    auto& extensionStatement =
        common::ku_dynamic_cast<const BoundStatement&, const BoundExtensionStatement&>(statement);
    auto logicalExtension = std::make_shared<LogicalExtension>(
        extensionStatement.getAction(), extensionStatement.getPath());
    return getSimplePlan(std::move(logicalExtension));
}

} // namespace planner
} // namespace kuzu
