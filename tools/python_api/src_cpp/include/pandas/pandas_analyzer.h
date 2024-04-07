#pragma once

#include "common/types/types.h"
#include "pybind_include.h"

namespace kuzu {

struct PythonGILWrapper {
    py::gil_scoped_acquire acquire;
};

class PandasAnalyzer {
public:
    PandasAnalyzer() { analyzedType = common::LogicalType{common::LogicalTypeID::ANY}; }

public:
    common::LogicalType getListType(py::object& ele, bool& canConvert);
    common::LogicalType getItemType(py::object ele, bool& canConvert);
    bool analyze(py::object column);
    common::LogicalType getAnalyzedType() { return analyzedType; }

private:
    common::LogicalType innerAnalyze(py::object column, bool& canConvert);

private:
    PythonGILWrapper gil;
    common::LogicalType analyzedType;
};

} // namespace kuzu
