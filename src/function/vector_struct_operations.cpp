#include "function/struct/vector_struct_operations.h"

namespace kuzu {
namespace function {

void VectorStructOperations::StructPack(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    common::ValueVector& result) {
    assert(!parameters.empty() && result.dataType.typeID == common::STRUCT);
    for (auto& parameter : parameters) {
        result.addChildVector(parameter);
    }
}

void StructPackVectorOperations::structPackBindFunc(const binder::expression_vector& arguments,
    kuzu::function::FunctionDefinition* definition, common::DataType& actualReturnType) {
    actualReturnType.extraTypeInfo = std::make_unique<common::StructTypeInfo>();
    actualReturnType.typeID = common::STRUCT;
    for (auto& argument : arguments) {
        reinterpret_cast<common::StructTypeInfo*>(actualReturnType.getExtraTypeInfo())
            ->addChildType(argument->getAlias(), argument->getDataType());
    }
    definition->returnTypeID = common::STRUCT;
}

} // namespace function
} // namespace kuzu
