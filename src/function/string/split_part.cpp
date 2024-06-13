#include "common/string_utils.h"
#include "function/string/vector_string_functions.h"

namespace kuzu {
namespace function {

using namespace kuzu::common;

struct SplitPart {
    static void operation(ku_string_t& strToSplit, ku_string_t& separator, int64_t idx,
        ku_string_t& result, ValueVector& resultVector) {
        auto splitStrVec = StringUtils::split(strToSplit.getAsString(), separator.getAsString());
        bool idxOutOfRange = idx <= 0 || (uint64_t)idx > splitStrVec.size();
        std::string resultStr = idxOutOfRange ? "" : splitStrVec[idx - 1];
        StringVector::addString(&resultVector, result, resultStr);
    }
};

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    return FunctionBindData::getSimpleBindData(arguments, LogicalType::STRING());
}

function_set SplitPartFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::TernaryStringExecFunction<ku_string_t, ku_string_t, int64_t, ku_string_t,
            SplitPart>,
        bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
