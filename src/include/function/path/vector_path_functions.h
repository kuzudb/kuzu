#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct NodesFunction {
    static constexpr const char* name = "NODES";

    static function_set getFunctionSet();
};

struct RelsFunction {
    static constexpr const char* name = "RELS";

    static function_set getFunctionSet();
};

struct PropertiesBindData : public FunctionBindData {
    common::idx_t childIdx;

    PropertiesBindData(common::LogicalType dataType, common::idx_t childIdx)
        : FunctionBindData{std::move(dataType)}, childIdx{childIdx} {}

    inline std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<PropertiesBindData>(resultType.copy(), childIdx);
    }
};

struct PropertiesFunction {
    static constexpr const char* name = "PROPERTIES";

    static function_set getFunctionSet();
};

struct IsTrailFunction {
    static constexpr const char* name = "IS_TRAIL";

    static function_set getFunctionSet();
};

struct IsACyclicFunction {
    static constexpr const char* name = "IS_ACYCLIC";

    static function_set getFunctionSet();
};

struct LengthFunction {
    static constexpr const char* name = "LENGTH";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
