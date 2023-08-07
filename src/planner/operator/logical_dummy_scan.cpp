#include "planner/logical_plan/scan/logical_dummy_scan.h"

namespace kuzu {
namespace planner {

void LogicalDummyScan::computeFactorizedSchema() {
    createEmptySchema();
    schema->createGroup();
}

void LogicalDummyScan::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
}

std::shared_ptr<binder::Expression> LogicalDummyScan::getDummyExpression() {
    auto logicalType = common::LogicalType(common::LogicalTypeID::STRING);
    auto nullValue = common::Value::createNullValue(logicalType);
    return std::make_shared<binder::LiteralExpression>(
        nullValue.copy(), common::InternalKeyword::PLACE_HOLDER);
}

} // namespace planner
} // namespace kuzu
