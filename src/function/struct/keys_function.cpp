
#include "function/scalar_function.h"
#include "function/struct/vector_struct_functions.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

struct KeysBindData : public FunctionBindData {
    std::vector<std::string> fields;

    KeysBindData(std::vector<LogicalType> paramTypes, LogicalType resultType,
        std::vector<std::string> fields)
        : FunctionBindData{std::move(paramTypes), std::move(resultType)},
          fields{std::move(fields)} {}

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<KeysBindData>(LogicalType::copy(paramTypes), resultType.copy(),
            fields);
    }
};

struct Keys {
    static void operation(list_entry_t& result, ValueVector& resultVector, void* dataPtr) {
        auto keysBindData = reinterpret_cast<KeysBindData*>(dataPtr);
        result = common::ListVector::addList(&resultVector, keysBindData->fields.size());
        auto dataVec = ListVector::getDataVector(&resultVector);
        for (auto i = 0u; i < keysBindData->fields.size(); i++) {
            dataVec->setValue(result.offset + i, keysBindData->fields[i]);
        }
    }
};

std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(input.arguments[0]->getDataType().copy());
    auto fields = common::StructType::getFieldNames(input.arguments[0]->dataType);
    fields.erase(std::remove_if(fields.begin(), fields.end(),
                     [&](const auto& item) {
                         return item == InternalKeyword::ID || item == InternalKeyword::LABEL ||
                                item == InternalKeyword::SRC || item == InternalKeyword::DST;
                     }),
        fields.end());
    return std::make_unique<KeysBindData>(std::move(paramTypes),
        LogicalType::LIST(LogicalType::STRING()), std::move(fields));
}

static std::unique_ptr<ScalarFunction> getKeysFunction(LogicalTypeID logicalTypeID) {
    return std::make_unique<ScalarFunction>(KeysFunctions::name,
        std::vector<LogicalTypeID>{logicalTypeID}, LogicalTypeID::LIST,
        ScalarFunction::UnaryExecStructFunction<struct_entry_t, list_entry_t, Keys>, bindFunc);
}

function_set KeysFunctions::getFunctionSet() {
    function_set functions;
    auto inputTypeIDs = std::vector<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL};
    for (auto inputTypeID : inputTypeIDs) {
        functions.push_back(getKeysFunction(inputTypeID));
    }
    return functions;
}

} // namespace function
} // namespace kuzu
