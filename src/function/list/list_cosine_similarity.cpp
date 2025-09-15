#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/type_utils.h"
#include "function/list/functions/list_function_utils.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ListCosineSimilarity {
    template <typename T>
    static void operation(common::list_entry_t& left, common::list_entry_t& right, T& result, common::ValueVector& leftVector,
        common::ValueVector& rightVector, common::ValueVector& /*resultVector*/) {
        auto leftDataVector = common::ListVector::getDataVector(&leftVector);
        auto rightDataVector = common::ListVector::getDataVector(&rightVector);
        result = 0;
        // for test, returning the sum of elements in 2 lists
        for (auto i=0u; i < left.size(); i++) {
            if (leftDataVector->isNull(left.offset + i)) {
                continue;
            }
            result += leftDataVector->getValue<T>(left.offset + i);
        }
        for (auto i=0u; i < right.size(); i++) {
            if (rightDataVector->isNull(right.offset + i)) {
                continue;
            }
            result +=rightataVector->getValue<T>(right.offset + i);
        }
    }
};

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    std::vector<LogicalType> types;
    auto scalarFunction = input.definition->ptrCast<ScalarFunction>();
    types.push_back(input.arguments[0]->getDataType().copy());
    types.push_back(input.arguments[1]->getDataType().copy());
    const auto& resultType = ListType::getChildType(input.arguments[0]->dataType);
    // justify datatypes
    if ((types[0] != types[1])||
        (types[0].getLogicalTypeID() == LogicalType::INT64().getLogicalTypeID())) {
            throw BinderException(stringFormat("Unsupported inner data type for {}: {}",
                input.definition->name, 
                types[0].getLogicalTypeID() == LogicalType::INT64().getLogicalTypeID()? 
                LogicalTypeUtils::toString(types[1].getLogicalTypeID()): LogicalTypeUtils::toString(types[0].getLogicalTypeID())
            ));
    }
    return std::make_unique<FunctionBindData>(std::move(types), resultType);
}

function_set ListConsineSimilarityFunction::getFunctionSet() {
    function_set result;
    auto execFunc = ScalarFunction::BinaryExecFunction<list_entry_t, list_entry_t, int64_t, ListCosineSimilarity>;
    auto function = std::make_unique<ScalarFunction>(name, 
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST}, LogicalTypeID::INT64, execFunc);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
