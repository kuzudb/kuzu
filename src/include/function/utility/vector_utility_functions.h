#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct CoalesceFunction {
    static constexpr const char* name = "COALESCE";

    static function_set getFunctionSet();
};

struct IfNullFunction {
    static constexpr const char* name = "IFNULL";

    static function_set getFunctionSet();
};

struct ConstantOrNullFunction {
    static constexpr const char* name = "CONSTANT_OR_NULL";

    static function_set getFunctionSet();
};

struct CountIfFunction {
    static constexpr const char* name = "COUNT_IF";

    static function_set getFunctionSet();
};

struct ErrorFunction {
    static constexpr const char* name = "ERROR";

    static function_set getFunctionSet();
};

struct NullIfFunction {
    static constexpr const char* name = "NULLIF";

    static function_set getFunctionSet();
};

struct TypeOfFunction {
    static constexpr const char* name = "TYPEOF";

    static function_set getFunctionSet();
};

struct PathPropertiesBindData : public FunctionBindData {
    common::idx_t childIdx;

    PathPropertiesBindData(common::LogicalType dataType, common::idx_t childIdx)
        : FunctionBindData{std::move(dataType)}, childIdx{childIdx} {}

    inline std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<PathPropertiesBindData>(resultType.copy(), childIdx);
    }
};

struct StructPropertiesBindData : public FunctionBindData {
    std::vector<common::idx_t> childIdxs;

    StructPropertiesBindData(common::LogicalType dataType, std::vector<common::idx_t> childIdxs)
        : FunctionBindData{std::move(dataType)}, childIdxs{std::move(childIdxs)} {}

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<StructPropertiesBindData>(resultType.copy(), childIdxs);
    }
};

struct PropertiesFunctions {
    static constexpr const char* name = "PROPERTIES";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
