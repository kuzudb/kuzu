#include "planner/operator/scan/logical_dummy_scan.h"

#include <memory>

#include "binder/expression/expression.h"
#include "binder/expression/literal_expression.h"
#include "common/constants.h"
#include "common/types/types.h"
#include "common/types/value/value.h"

using namespace kuzu::common;

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
    auto logicalType = LogicalType(LogicalTypeID::STRING);
    auto nullValue = Value::createNullValue(logicalType);
    return std::make_shared<binder::LiteralExpression>(
        nullValue.copy(), InternalKeyword::PLACE_HOLDER);
}

} // namespace planner
} // namespace kuzu
