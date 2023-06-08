#pragma once

#include "function/vector_operations.h"

namespace kuzu {
namespace function {
class VectorDateOperations : public VectorOperations {};

struct DatePartVectorOperation : public VectorDateOperations {
    static vector_operation_definitions getDefinitions();
};

struct DateTruncVectorOperation : public VectorDateOperations {
    static vector_operation_definitions getDefinitions();
};

struct DayNameVectorOperation : public VectorDateOperations {
    static vector_operation_definitions getDefinitions();
};

struct GreatestVectorOperation : public VectorDateOperations {
    static vector_operation_definitions getDefinitions();
};

struct LastDayVectorOperation : public VectorDateOperations {
    static vector_operation_definitions getDefinitions();
};

struct LeastVectorOperation : public VectorDateOperations {
    static vector_operation_definitions getDefinitions();
};

struct MakeDateVectorOperation : public VectorDateOperations {
    static vector_operation_definitions getDefinitions();
};

struct MonthNameVectorOperation : public VectorDateOperations {
    static vector_operation_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
