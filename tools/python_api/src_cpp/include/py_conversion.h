#pragma once

#include <cstdint>

#include "common/exception/conversion.h"
#include "common/types/value/value.h"
#include "common/vector/value_vector.h"
#include "pybind_include.h"
#include <datetime.h>

namespace kuzu {

enum class PythonObjectType : uint8_t {
    None,
    Integer,
    Float,
    Bool,
    Datetime,
    Date,
    String,
    List,
};

PythonObjectType getPythonObjectType(py::handle& ele);

void tryTransformPythonNumeric(common::ValueVector* outputVector, uint64_t pos, py::handle ele);

void transformListValue(common::ValueVector* outputVector, uint64_t pos, py::handle ele);

void transformPythonValue(common::ValueVector* outputVector, uint64_t pos, py::handle ele);

} // namespace kuzu
