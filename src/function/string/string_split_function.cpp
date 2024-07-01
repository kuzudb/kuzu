#include "common/string_utils.h"
#include "function/string/vector_string_functions.h"

namespace kuzu {
namespace function {

using namespace kuzu::common;

struct StringSplit {
    static void operation(ku_string_t& strToSplit, ku_string_t& separator, list_entry_t& result,
        ValueVector& resultVector) {
        auto splitStrVec = StringUtils::split(strToSplit.getAsString(), separator.getAsString());
        result = ListVector::addList(&resultVector, splitStrVec.size());
        for (auto i = 0u; i < result.size; i++) {
            ListVector::getDataVector(&resultVector)->setValue(result.offset + i, splitStrVec[i]);
        }
    }
};

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    return FunctionBindData::getSimpleBindData(arguments, LogicalType::LIST(LogicalType::STRING()));
}

function_set StringSplitFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::LIST,
        ScalarFunction::BinaryStringExecFunction<ku_string_t, ku_string_t, list_entry_t,
            StringSplit>,
        bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
