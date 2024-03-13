#pragma once

#include "common/types/int128_t.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct AddFunction {
    static function_set getFunctionSet();
};

struct SubtractFunction {
    static function_set getFunctionSet();
};

struct MultiplyFunction {
    static function_set getFunctionSet();
};

struct DivideFunction {
    static function_set getFunctionSet();
};

struct ModuloFunction {
    static function_set getFunctionSet();
};

struct PowerFunction {
    static function_set getFunctionSet();
};

struct AbsFunction {
    static constexpr const char* name = "abs";

    static function_set getFunctionSet();
};

struct AcosFunction {
    static function_set getFunctionSet();
};

struct AsinFunction {
    static function_set getFunctionSet();
};

struct AtanFunction {
    static function_set getFunctionSet();
};

struct Atan2Function {
    static function_set getFunctionSet();
};

struct BitwiseXorFunction {
    static function_set getFunctionSet();
};

struct BitwiseAndFunction {
    static function_set getFunctionSet();
};

struct BitwiseOrFunction {
    static function_set getFunctionSet();
};

struct BitShiftLeftFunction {
    static function_set getFunctionSet();
};

struct BitShiftRightFunction {
    static function_set getFunctionSet();
};

struct CbrtFunction {
    static function_set getFunctionSet();
};

struct CeilFunction {
    static function_set getFunctionSet();
};

struct CosFunction {
    static function_set getFunctionSet();
};

struct CotFunction {
    static function_set getFunctionSet();
};

struct DegreesFunction {
    static function_set getFunctionSet();
};

struct EvenFunction {
    static function_set getFunctionSet();
};

struct FactorialFunction {
    static function_set getFunctionSet();
};

struct FloorFunction {
    static function_set getFunctionSet();
};

struct GammaFunction {
    static function_set getFunctionSet();
};

struct LgammaFunction {
    static function_set getFunctionSet();
};

struct LnFunction {
    static function_set getFunctionSet();
};

struct LogFunction {
    static function_set getFunctionSet();
};

struct Log2Function {
    static function_set getFunctionSet();
};

struct NegateFunction {
    static function_set getFunctionSet();
};

struct PiFunction {
    static function_set getFunctionSet();
};

struct RadiansFunction {
    static function_set getFunctionSet();
};

struct RoundFunction {
    static function_set getFunctionSet();
};

struct SinFunction {
    static function_set getFunctionSet();
};

struct SignFunction {
    static function_set getFunctionSet();
};

struct SqrtFunction {
    static function_set getFunctionSet();
};

struct TanFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
