#include "planner/logical_plan/logical_operator/logical_dummy_scan.h"

namespace kuzu {
namespace planner {

void LogicalDummyScan::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    //    schema->setGroupAsSingleState(groupPos); // Mark group holding constant as single state.
    //    schema->insertToGroupAndScope(getDummyExpression(), groupPos);
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
