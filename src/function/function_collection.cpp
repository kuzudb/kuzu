#include "function/function_collection.h"

#include "function/aggregate/collect.h"
#include "function/aggregate/count.h"
#include "function/aggregate/count_star.h"
#include "function/arithmetic/vector_arithmetic_functions.h"
#include "function/array/vector_array_functions.h"
#include "function/blob/vector_blob_functions.h"
#include "function/cast/vector_cast_functions.h"
#include "function/comparison/vector_comparison_functions.h"
#include "function/date/vector_date_functions.h"
#include "function/interval/vector_interval_functions.h"
#include "function/list/vector_list_functions.h"
#include "function/map/vector_map_functions.h"
#include "function/path/vector_path_functions.h"
#include "function/rdf/vector_rdf_functions.h"
#include "function/schema/vector_node_rel_functions.h"
#include "function/string/vector_string_functions.h"
#include "function/struct/vector_struct_functions.h"
#include "function/timestamp/vector_timestamp_functions.h"
#include "function/union/vector_union_functions.h"
#include "function/uuid/vector_uuid_functions.h"

namespace kuzu {
namespace function {

#define SCALAR_FUNCTION(_PARAM)                                                                    \
    { _PARAM::getFunctionSet, _PARAM::name, CatalogEntryType::SCALAR_FUNCTION_ENTRY }
#define SCALAR_FUNCTION_ALIAS(_PARAM)                                                              \
    { _PARAM::getFunctionSet, _PARAM::alias, CatalogEntryType::SCALAR_FUNCTION_ENTRY }
#define REWRITE_FUNCTION(_PARAM)                                                                   \
    { _PARAM::getFunctionSet, _PARAM::name, CatalogEntryType::REWRITE_FUNCTION_ENTRY }
#define AGGREGATE_FUNCTION(_PARAM)                                                                 \
    { _PARAM::getFunctionSet, _PARAM::name, CatalogEntryType::AGGREGATE_FUNCTION_ENTRY }
#define FINAL_FUNCTION                                                                             \
    { nullptr, nullptr, CatalogEntryType::SCALAR_FUNCTION_ENTRY }

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

        // Array Functions
        SCALAR_FUNCTION(ArrayValueFunction), SCALAR_FUNCTION(ArrayCrossProductFunction),
        SCALAR_FUNCTION(ArrayCosineSimilarityFunction), SCALAR_FUNCTION(ArrayDistanceFunction),
        SCALAR_FUNCTION(ArrayInnerProductFunction), SCALAR_FUNCTION(ArrayDotProductFunction),

        // List functions
        SCALAR_FUNCTION(ListCreationFunction), SCALAR_FUNCTION(ListRangeFunction),
        SCALAR_FUNCTION(ListExtractFunction), SCALAR_FUNCTION_ALIAS(ListExtractFunction),
        SCALAR_FUNCTION(ListConcatFunction), SCALAR_FUNCTION_ALIAS(ListConcatFunction),
        SCALAR_FUNCTION(ArrayConcatFunction), SCALAR_FUNCTION_ALIAS(ArrayConcatFunction),
        SCALAR_FUNCTION(ListAppendFunction), SCALAR_FUNCTION(ArrayAppendFunction),
        SCALAR_FUNCTION_ALIAS(ArrayAppendFunction), SCALAR_FUNCTION(ListPrependFunction),
        SCALAR_FUNCTION(ArrayPrependFunction), SCALAR_FUNCTION_ALIAS(ArrayPrependFunction),
        SCALAR_FUNCTION(ListPositionFunction), SCALAR_FUNCTION_ALIAS(ListPositionFunction),
        SCALAR_FUNCTION(ArrayPositionFunction), SCALAR_FUNCTION_ALIAS(ArrayPositionFunction),
        SCALAR_FUNCTION(ListContainsFunction), SCALAR_FUNCTION_ALIAS(ListContainsFunction),
        SCALAR_FUNCTION(ArrayContainsFunction), SCALAR_FUNCTION_ALIAS(ArrayContainsFunction),
        SCALAR_FUNCTION(ListSliceFunction), SCALAR_FUNCTION(ArraySliceFunction),
        SCALAR_FUNCTION(ListSortFunction), SCALAR_FUNCTION(ListReverseSortFunction),
        SCALAR_FUNCTION(ListSumFunction), SCALAR_FUNCTION(ListProductFunction),
        SCALAR_FUNCTION(ListDistinctFunction), SCALAR_FUNCTION(ListUniqueFunction),
        SCALAR_FUNCTION(ListAnyValueFunction), SCALAR_FUNCTION(ListReverseFunction),
        SCALAR_FUNCTION(SizeFunction),

        // Cast functions
        SCALAR_FUNCTION(CastToDateFunction), SCALAR_FUNCTION_ALIAS(CastToDateFunction),
        SCALAR_FUNCTION(CastToTimestampFunction), SCALAR_FUNCTION(CastToIntervalFunction),
        SCALAR_FUNCTION_ALIAS(CastToIntervalFunction), SCALAR_FUNCTION(CastToStringFunction),
        SCALAR_FUNCTION_ALIAS(CastToStringFunction), SCALAR_FUNCTION(CastToBlobFunction),
        SCALAR_FUNCTION_ALIAS(CastToBlobFunction), SCALAR_FUNCTION(CastToUUIDFunction),
        SCALAR_FUNCTION_ALIAS(CastToUUIDFunction), SCALAR_FUNCTION(CastToDoubleFunction),
        SCALAR_FUNCTION(CastToFloatFunction), SCALAR_FUNCTION(CastToSerialFunction),
        SCALAR_FUNCTION(CastToInt64Function), SCALAR_FUNCTION(CastToInt32Function),
        SCALAR_FUNCTION(CastToInt16Function), SCALAR_FUNCTION(CastToInt8Function),
        SCALAR_FUNCTION(CastToUInt64Function), SCALAR_FUNCTION(CastToUInt32Function),
        SCALAR_FUNCTION(CastToUInt16Function), SCALAR_FUNCTION(CastToUInt8Function),
        SCALAR_FUNCTION(CastToInt128Function), SCALAR_FUNCTION(CastToBoolFunction),
        SCALAR_FUNCTION(CastAnyFunction),

        // Comparison functions
        SCALAR_FUNCTION(EqualsFunction), SCALAR_FUNCTION(NotEqualsFunction),
        SCALAR_FUNCTION(GreaterThanFunction), SCALAR_FUNCTION(GreaterThanEqualsFunction),
        SCALAR_FUNCTION(LessThanFunction), SCALAR_FUNCTION(LessThanEqualsFunction),

        // Date functions
        SCALAR_FUNCTION(DatePartFunction), SCALAR_FUNCTION_ALIAS(DatePartFunction),
        SCALAR_FUNCTION(DateTruncFunction), SCALAR_FUNCTION_ALIAS(DateTruncFunction),
        SCALAR_FUNCTION(DayNameFunction), SCALAR_FUNCTION(GreatestFunction),
        SCALAR_FUNCTION(LastDayFunction), SCALAR_FUNCTION(LeastFunction),
        SCALAR_FUNCTION(MakeDateFunction), SCALAR_FUNCTION(MonthNameFunction),

        // Timestamp functions
        SCALAR_FUNCTION(CenturyFunction), SCALAR_FUNCTION(EpochMsFunction),
        SCALAR_FUNCTION(ToTimestampFunction),

        // Interval functions
        SCALAR_FUNCTION(ToYearsFunction), SCALAR_FUNCTION(ToMonthsFunction),
        SCALAR_FUNCTION(ToDaysFunction), SCALAR_FUNCTION(ToHoursFunction),
        SCALAR_FUNCTION(ToMinutesFunction), SCALAR_FUNCTION(ToSecondsFunction),
        SCALAR_FUNCTION(ToMillisecondsFunction), SCALAR_FUNCTION(ToMicrosecondsFunction),

        // Blob functions
        SCALAR_FUNCTION(OctetLengthFunctions), SCALAR_FUNCTION(EncodeFunctions),
        SCALAR_FUNCTION(DecodeFunctions),

        // UUID functions
        SCALAR_FUNCTION(GenRandomUUIDFunction),

        // Struct functions
        SCALAR_FUNCTION(StructPackFunctions), SCALAR_FUNCTION(StructExtractFunctions),

        // Map functions
        SCALAR_FUNCTION(MapCreationFunctions), SCALAR_FUNCTION(MapExtractFunctions),
        SCALAR_FUNCTION_ALIAS(MapExtractFunctions), SCALAR_FUNCTION_ALIAS(SizeFunction),
        SCALAR_FUNCTION(MapKeysFunctions), SCALAR_FUNCTION(MapValuesFunctions),

        // Union functions
        SCALAR_FUNCTION(UnionValueFunction), SCALAR_FUNCTION(UnionTagFunction),
        SCALAR_FUNCTION(UnionExtractFunction),

        // Node/rel functions
        SCALAR_FUNCTION(OffsetFunction), REWRITE_FUNCTION(IDFunction),

        // Path functions
        SCALAR_FUNCTION(NodesFunction), SCALAR_FUNCTION(RelsFunction),
        SCALAR_FUNCTION(PropertiesFunction), SCALAR_FUNCTION(IsTrailFunction),
        SCALAR_FUNCTION(IsACyclicFunction),

        // Rdf functions
        SCALAR_FUNCTION(RDFTypeFunction), SCALAR_FUNCTION(ValidatePredicateFunction),

        // Aggregate functions
        AGGREGATE_FUNCTION(CountStarFunction), AGGREGATE_FUNCTION(CountFunction),
        AGGREGATE_FUNCTION(AggregateSumFunction), AGGREGATE_FUNCTION(AggregateAvgFunction),
        AGGREGATE_FUNCTION(AggregateMinFunction), AGGREGATE_FUNCTION(AggregateMaxFunction),
        AGGREGATE_FUNCTION(CollectFunction),

        // End of array
        FINAL_FUNCTION};

    return functions;
}

} // namespace function
} // namespace kuzu
