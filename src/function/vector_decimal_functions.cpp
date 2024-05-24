#include "function/decimal/vector_decimal_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

using param_get_func_t = std::function<std::pair<int, int>(int, int, int, int)>;

static std::unique_ptr<FunctionBindData> genericArithmeticFunc(
    const binder::expression_vector& arguments, Function*, param_get_func_t getParams) {
    if (arguments[0]->getDataType() != arguments[1]->getDataType()) {
        auto precision1 = DecimalType::getPrecision(arguments[0]->getDataType());
        auto precision2 = DecimalType::getPrecision(arguments[1]->getDataType());
        auto scale1 = DecimalType::getScale(arguments[0]->getDataType());
        auto scale2 = DecimalType::getScale(arguments[1]->getDataType());
        auto params = getParams(precision1, precision2, scale1, scale2);
        return FunctionBindData::getSimpleBindData(arguments,
            *LogicalType::DECIMAL(params.first, params.second));
    }
    return FunctionBindData::getSimpleBindData(arguments, arguments[0]->getDataType());
}

// resulting param func rules are from
// https://learn.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql
static std::pair<int, int> resultingAddParams(int p1, int p2, int s1, int s2) {
    auto p = std::min(38, std::max(s1, s2) + std::max(p1 - s1, p2 - s2) + 1);
    auto s = std::min(p, std::max(s1, s2));
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindAddFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericArithmeticFunc(arguments, func, resultingAddParams);
}

static std::pair<int, int> resultingSubtractParams(int p1, int p2, int s1, int s2) {
    auto p = std::min(38, std::max(s1, s2) + std::max(p1 - s1, p2 - s2) + 1);
    auto s = std::min(p, std::max(s1, s2));
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindSubtractFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericArithmeticFunc(arguments, func, resultingSubtractParams);
}

static std::pair<int, int> resultingMultiplyParams(int p1, int p2, int s1, int s2) {
    auto p = std::min(38, p1 + p2 + 1);
    auto s = std::min(p, s1 + s2);
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindMultiplyFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericArithmeticFunc(arguments, func, resultingMultiplyParams);
}

static std::pair<int, int> resultingDivideParams(int p1, int p2, int s1, int s2) {
    auto p = std::min(38, p1 - s1 + s2 + std::max(6, s1 + p2 + 1));
    auto s = std::min(p, std::max(6, s1 + p2 + 1));
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindDivideFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericArithmeticFunc(arguments, func, resultingDivideParams);
}

static std::pair<int, int> resultingModuloParams(int p1, int p2, int s1, int s2) {
    auto p = std::min(38, std::min(p1 - s1, p2 - s2) + std::max(s1, s2));
    auto s = std::min(p, std::max(s1, s2));
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindModuloFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericArithmeticFunc(arguments, func, resultingModuloParams);
}

} // namespace function
} // namespace kuzu
