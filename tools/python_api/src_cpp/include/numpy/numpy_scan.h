#pragma once

#include "pybind_include.h"

namespace kuzu {

namespace common {
class ValueVector;
} // namespace common

struct PandasColumnBindData;

struct NumpyScan {
    static void scan(PandasColumnBindData* bindData, uint64_t count, uint64_t offset,
        common::ValueVector* outputVector);
    static void scanObjectColumn(PyObject** col, uint64_t count, uint64_t offset,
        common::ValueVector* outputVector);
};

} // namespace kuzu
