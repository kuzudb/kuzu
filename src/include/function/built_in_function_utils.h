#pragma once

#include "aggregate_function.h"
#include "scalar_function.h"

namespace kuzu {
namespace catalog {
class CatalogSet;
} // namespace catalog

namespace function {

class BuiltInFunctionsUtils {
public:
    static void createFunctions(catalog::CatalogSet* catalogSet);

    static Function* matchFunction(const std::string& name, catalog::CatalogSet* catalogSet);
    // TODO(Ziyi): We should have a unified interface for matching table, aggregate and scalar
    // functions.
    static Function* matchFunction(const std::string& name,
        const std::vector<common::LogicalType>& inputTypes, catalog::CatalogSet* catalogSet);

    static AggregateFunction* matchAggregateFunction(const std::string& name,
        const std::vector<common::LogicalType>& inputTypes, bool isDistinct,
        catalog::CatalogSet* catalogSet);

    static uint32_t getCastCost(
        common::LogicalTypeID inputTypeID, common::LogicalTypeID targetTypeID);

private:
    // TODO(Xiyang): move casting cost related functions to binder.
    static uint32_t getTargetTypeCost(common::LogicalTypeID typeID);

    static uint32_t castInt64(common::LogicalTypeID targetTypeID);

    static uint32_t castInt32(common::LogicalTypeID targetTypeID);

    static uint32_t castInt16(common::LogicalTypeID targetTypeID);

    static uint32_t castInt8(common::LogicalTypeID targetTypeID);

    static uint32_t castUInt64(common::LogicalTypeID targetTypeID);

    static uint32_t castUInt32(common::LogicalTypeID targetTypeID);

    static uint32_t castUInt16(common::LogicalTypeID targetTypeID);

    static uint32_t castUInt8(common::LogicalTypeID targetTypeID);

    static uint32_t castInt128(common::LogicalTypeID targetTypeID);

    static uint32_t castDouble(common::LogicalTypeID targetTypeID);

    static uint32_t castFloat(common::LogicalTypeID targetTypeID);

    static uint32_t castDate(common::LogicalTypeID targetTypeID);

    static uint32_t castSerial(common::LogicalTypeID targetTypeID);

    static uint32_t castTimestamp(common::LogicalTypeID targetTypeID);

    static uint32_t castFromString(common::LogicalTypeID inputTypeID);

    static uint32_t castFromRDFVariant(common::LogicalTypeID inputTypeID);

    static uint32_t castUUID(common::LogicalTypeID targetTypeID);

    static Function* getBestMatch(std::vector<Function*>& functions);

    static uint32_t getFunctionCost(
        const std::vector<common::LogicalType>& inputTypes, Function* function, bool isOverload);
    static uint32_t matchParameters(const std::vector<common::LogicalType>& inputTypes,
        const std::vector<common::LogicalTypeID>& targetTypeIDs, bool isOverload);
    static uint32_t matchVarLengthParameters(const std::vector<common::LogicalType>& inputTypes,
        common::LogicalTypeID targetTypeID, bool isOverload);
    static uint32_t getAggregateFunctionCost(const std::vector<common::LogicalType>& inputTypes,
        bool isDistinct, AggregateFunction* function);

    static void validateSpecialCases(std::vector<Function*>& candidateFunctions,
        const std::string& name, const std::vector<common::LogicalType>& inputTypes,
        function::function_set& set);

    // Scalar functions.
    static void registerScalarFunctions(catalog::CatalogSet* catalogSet);
    static void registerComparisonFunctions(catalog::CatalogSet* catalogSet);
    static void registerArithmeticFunctions(catalog::CatalogSet* catalogSet);
    static void registerDateFunctions(catalog::CatalogSet* catalogSet);
    static void registerTimestampFunctions(catalog::CatalogSet* catalogSet);
    static void registerIntervalFunctions(catalog::CatalogSet* catalogSet);
    static void registerBlobFunctions(catalog::CatalogSet* catalogSet);
    static void registerUUIDFunctions(catalog::CatalogSet* catalogSet);
    static void registerStringFunctions(catalog::CatalogSet* catalogSet);
    static void registerCastFunctions(catalog::CatalogSet* catalogSet);
    static void registerListFunctions(catalog::CatalogSet* catalogSet);
    static void registerStructFunctions(catalog::CatalogSet* catalogSet);
    static void registerMapFunctions(catalog::CatalogSet* catalogSet);
    static void registerUnionFunctions(catalog::CatalogSet* catalogSet);
    static void registerNodeRelFunctions(catalog::CatalogSet* catalogSet);
    static void registerPathFunctions(catalog::CatalogSet* catalogSet);
    static void registerRdfFunctions(catalog::CatalogSet* catalogSet);

    // Aggregate functions.
    static void registerAggregateFunctions(catalog::CatalogSet* catalogSet);
    static void registerCountStar(catalog::CatalogSet* catalogSet);
    static void registerCount(catalog::CatalogSet* catalogSet);
    static void registerSum(catalog::CatalogSet* catalogSet);
    static void registerAvg(catalog::CatalogSet* catalogSet);
    static void registerMin(catalog::CatalogSet* catalogSet);
    static void registerMax(catalog::CatalogSet* catalogSet);
    static void registerCollect(catalog::CatalogSet* catalogSet);

    // Table functions.
    static void registerTableFunctions(catalog::CatalogSet* catalogSet);

    // Validations
    static void validateNonEmptyCandidateFunctions(
        std::vector<AggregateFunction*>& candidateFunctions, const std::string& name,
        const std::vector<common::LogicalType>& inputTypes, bool isDistinct,
        function::function_set& set);
    static void validateNonEmptyCandidateFunctions(std::vector<Function*>& candidateFunctions,
        const std::string& name, const std::vector<common::LogicalType>& inputTypes,
        function::function_set& set);
};

} // namespace function
} // namespace kuzu
