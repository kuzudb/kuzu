#include "function/function_collection.h"

#include "function/arithmetic/vector_arithmetic_functions.h"
#include "function/array/vector_array_functions.h"
#include "function/string/vector_string_functions.h"

namespace kuzu {
namespace function {

#define SCALAR_FUNCTION(_PARAM)                                                                    \
    { _PARAM::name, _PARAM::getFunctionSet }
#define SCALAR_FUNCTION_ALIAS(_PARAM)                                                              \
    { _PARAM::alias, _PARAM::getFunctionSet }
#define FINAL_FUNCTION                                                                             \
    { nullptr, nullptr }

FunctionCollection* FunctionCollection::getFunctions() {
    static FunctionCollection functions[] = {

        // Arithmetic Functions
        SCALAR_FUNCTION(AddFunction), SCALAR_FUNCTION(SubtractFunction),
        SCALAR_FUNCTION(MultiplyFunction), SCALAR_FUNCTION(DivideFunction),
        SCALAR_FUNCTION(ModuloFunction), SCALAR_FUNCTION(PowerFunction),
        SCALAR_FUNCTION(AbsFunction), SCALAR_FUNCTION(AcosFunction), SCALAR_FUNCTION(AsinFunction),
        SCALAR_FUNCTION(AtanFunction), SCALAR_FUNCTION(Atan2Function),
        SCALAR_FUNCTION(BitwiseXorFunction), SCALAR_FUNCTION(BitwiseAndFunction),
        SCALAR_FUNCTION(BitwiseOrFunction), SCALAR_FUNCTION(BitShiftLeftFunction),
        SCALAR_FUNCTION(BitShiftRightFunction), SCALAR_FUNCTION(CbrtFunction),
        SCALAR_FUNCTION(CeilFunction), SCALAR_FUNCTION_ALIAS(CeilFunction),
        SCALAR_FUNCTION(CosFunction), SCALAR_FUNCTION(CotFunction),
        SCALAR_FUNCTION(DegreesFunction), SCALAR_FUNCTION(EvenFunction),
        SCALAR_FUNCTION(FactorialFunction), SCALAR_FUNCTION(FloorFunction),
        SCALAR_FUNCTION(GammaFunction), SCALAR_FUNCTION(LgammaFunction),
        SCALAR_FUNCTION(LnFunction), SCALAR_FUNCTION(LogFunction),
        SCALAR_FUNCTION_ALIAS(LogFunction), SCALAR_FUNCTION(Log2Function),
        SCALAR_FUNCTION(NegateFunction), SCALAR_FUNCTION(PiFunction),
        SCALAR_FUNCTION_ALIAS(PowerFunction), SCALAR_FUNCTION(RadiansFunction),
        SCALAR_FUNCTION(RoundFunction), SCALAR_FUNCTION(SinFunction), SCALAR_FUNCTION(SignFunction),
        SCALAR_FUNCTION(SqrtFunction), SCALAR_FUNCTION(TanFunction),

        // String Functions
        SCALAR_FUNCTION(ArrayExtractFunction), SCALAR_FUNCTION(ConcatFunction),
        SCALAR_FUNCTION(ContainsFunction), SCALAR_FUNCTION(LowerFunction),
        SCALAR_FUNCTION_ALIAS(LowerFunction), SCALAR_FUNCTION(LeftFunction),
        SCALAR_FUNCTION(LpadFunction), SCALAR_FUNCTION(LtrimFunction),
        SCALAR_FUNCTION(StartsWithFunction), SCALAR_FUNCTION_ALIAS(StartsWithFunction),
        SCALAR_FUNCTION(RepeatFunction), SCALAR_FUNCTION(ReverseFunction),
        SCALAR_FUNCTION(RightFunction), SCALAR_FUNCTION(RpadFunction),
        SCALAR_FUNCTION(RtrimFunction), SCALAR_FUNCTION(SubStrFunction),
        SCALAR_FUNCTION_ALIAS(SubStrFunction), SCALAR_FUNCTION(EndsWithFunction),
        SCALAR_FUNCTION_ALIAS(EndsWithFunction), SCALAR_FUNCTION(TrimFunction),
        SCALAR_FUNCTION(UpperFunction), SCALAR_FUNCTION_ALIAS(UpperFunction),
        SCALAR_FUNCTION(RegexpFullMatchFunction), SCALAR_FUNCTION(RegexpMatchesFunction),
        SCALAR_FUNCTION(RegexpReplaceFunction), SCALAR_FUNCTION(RegexpExtractFunction),
        SCALAR_FUNCTION(RegexpExtractAllFunction), SCALAR_FUNCTION(LevenshteinFunction),
        SCALAR_FUNCTION(ArrayValueFunction), SCALAR_FUNCTION(ArrayCrossProductFunction),
        SCALAR_FUNCTION(ArrayCosineSimilarityFunction), SCALAR_FUNCTION(ArrayDistanceFunction),
        SCALAR_FUNCTION(ArrayInnerProductFunction), SCALAR_FUNCTION(ArrayDotProductFunction),
        // End of array
        FINAL_FUNCTION};

    return functions;
}

} // namespace function
} // namespace kuzu
