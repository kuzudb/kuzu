#include "binder/bound_load_extension_statement.h"
#include "common/cast.h"
#include "planner/operator/logical_load_extension.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::planLoadExtension(const BoundStatement& statement) {
    auto& loadExtensionStatement =
        common::ku_dynamic_cast<const BoundStatement&, const BoundLoadExtensionStatement&>(
            statement);
    auto logicalLoadExtension =
        std::make_shared<LogicalLoadExtension>(loadExtensionStatement.getPath());
    return getSimplePlan(std::move(logicalLoadExtension));
}

} // namespace planner
} // namespace kuzu
