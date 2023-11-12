#include "numpy/numpy_type.h"

#include "common/string_utils.h"

namespace kuzu {

using namespace kuzu::common;

static NumpyNullableType convertNumpyTypeInternal(const std::string& colTypeStr) {
    if (colTypeStr == "bool" || colTypeStr == "boolean") {
        return NumpyNullableType::BOOL;
    }
    if (colTypeStr == "uint8" || colTypeStr == "UInt8") {
        return NumpyNullableType::UINT_8;
    }
    if (colTypeStr == "uint16" || colTypeStr == "UInt16") {
        return NumpyNullableType::UINT_16;
    }
    if (colTypeStr == "uint32" || colTypeStr == "UInt32") {
        return NumpyNullableType::UINT_32;
    }
    if (colTypeStr == "uint64" || colTypeStr == "UInt64") {
        return NumpyNullableType::UINT_64;
    }
    if (colTypeStr == "int8" || colTypeStr == "Int8") {
        return NumpyNullableType::INT_8;
    }
    if (colTypeStr == "int16" || colTypeStr == "Int16") {
        return NumpyNullableType::INT_16;
    }
    if (colTypeStr == "int32" || colTypeStr == "Int32") {
        return NumpyNullableType::INT_32;
    }
    if (colTypeStr == "int64" || colTypeStr == "Int64") {
        return NumpyNullableType::INT_64;
    }
    if (colTypeStr == "float16" || colTypeStr == "Float16") {
        return NumpyNullableType::FLOAT_16;
    }
    if (colTypeStr == "float32" || colTypeStr == "Float32") {
        return NumpyNullableType::FLOAT_32;
    }
    if (colTypeStr == "float64" || colTypeStr == "Float64") {
        return NumpyNullableType::FLOAT_64;
    }
    if (colTypeStr == "timedelta64[ns]") {
        return NumpyNullableType::TIMEDELTA;
    }
    // We use 'StartsWith' because it might have ', tz' at the end, indicating timezone.
    if (colTypeStr.starts_with("datetime64[us")) {
        return NumpyNullableType::DATETIME_US;
    }
    if (colTypeStr.starts_with("datetime64[ns")) {
        return NumpyNullableType::DATETIME_NS;
    }
    // LCOV_EXCL_START
    // Legacy datetime type indicators
    if (colTypeStr.starts_with("<M8[us")) {
        return NumpyNullableType::DATETIME_US;
    }
    if (colTypeStr.starts_with("<M8[ns")) {
        return NumpyNullableType::DATETIME_NS;
    }
    // LCOV_EXCL_STOP
    KU_UNREACHABLE;
}

NumpyType NumpyTypeUtils::convertNumpyType(const py::handle& colType) {
    auto colTypeStr = std::string(py::str(colType));
    NumpyType resultNPType;
    resultNPType.type = convertNumpyTypeInternal(colTypeStr);
    return resultNPType;
}

std::unique_ptr<LogicalType> NumpyTypeUtils::numpyToLogicalType(const NumpyType& col_type) {
    // TODO(Ziyi): timestamp with timezone is currently not supported.
    switch (col_type.type) {
    case NumpyNullableType::BOOL:
        return std::make_unique<LogicalType>(LogicalTypeID::BOOL);
    case NumpyNullableType::INT_8:
        return std::make_unique<LogicalType>(LogicalTypeID::INT8);
    case NumpyNullableType::INT_16:
        return std::make_unique<LogicalType>(LogicalTypeID::INT16);
    case NumpyNullableType::INT_32:
        return std::make_unique<LogicalType>(LogicalTypeID::INT32);
    case NumpyNullableType::INT_64:
        return std::make_unique<LogicalType>(LogicalTypeID::INT64);
    case NumpyNullableType::UINT_8:
        return std::make_unique<LogicalType>(LogicalTypeID::UINT8);
    case NumpyNullableType::UINT_16:
        return std::make_unique<LogicalType>(LogicalTypeID::UINT16);
    case NumpyNullableType::UINT_32:
        return std::make_unique<LogicalType>(LogicalTypeID::UINT32);
    case NumpyNullableType::UINT_64:
        return std::make_unique<LogicalType>(LogicalTypeID::UINT64);
    case NumpyNullableType::FLOAT_16:
    case NumpyNullableType::FLOAT_32:
        return std::make_unique<LogicalType>(LogicalTypeID::FLOAT);
    case NumpyNullableType::FLOAT_64:
        return std::make_unique<LogicalType>(LogicalTypeID::DOUBLE);
    case NumpyNullableType::TIMEDELTA:
        return std::make_unique<LogicalType>(LogicalTypeID::INTERVAL);
    case NumpyNullableType::DATETIME_US:
    case NumpyNullableType::DATETIME_NS:
        return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP);
    default:
        KU_UNREACHABLE;
    }
}

} // namespace kuzu
