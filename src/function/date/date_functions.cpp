#include "function/date/date_functions.h"

#include "main/client_context.h"

namespace kuzu {
namespace function {

void CurrentDate::operation(common::date_t& result, void* dataPtr) {
    auto currentTS =
        reinterpret_cast<FunctionBindData*>(dataPtr)->clientContext->getTx()->getCurrentTS();
    result = common::Timestamp::getDate(common::timestamp_tz_t(currentTS));
}

void CurrentTimestamp::operation(common::timestamp_tz_t& result, void* dataPtr) {
    auto currentTS =
        reinterpret_cast<FunctionBindData*>(dataPtr)->clientContext->getTx()->getCurrentTS();
    result = common::timestamp_tz_t(currentTS);
}

} // namespace function
} // namespace kuzu
