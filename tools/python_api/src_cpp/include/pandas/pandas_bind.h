#pragma once

#include "numpy/numpy_type.h"
#include "pandas_column.h"
#include "py_object_container.h"
#include "pybind_include.h"

namespace kuzu {

namespace main {
class ClientContext;
}

struct RegisteredArray {
    explicit RegisteredArray(py::array npArray) : npArray(std::move(npArray)) {}
    py::array npArray;
};

struct PandasColumnBindData {
    NumpyType npType;
    std::unique_ptr<PandasColumn> pandasCol;
    std::unique_ptr<RegisteredArray> mask;
    PythonObjectContainer objectStrValContainer;

    PandasColumnBindData() = default;

    PandasColumnBindData(NumpyType npType, std::unique_ptr<PandasColumn> pandasCol,
        std::unique_ptr<RegisteredArray> mask)
        : npType{npType}, pandasCol{std::move(pandasCol)}, mask{std::move(mask)} {}

    std::unique_ptr<PandasColumnBindData> copy() {
        py::gil_scoped_acquire acquire;
        return std::make_unique<PandasColumnBindData>(npType, pandasCol->copy(),
            mask == nullptr ? nullptr : std::make_unique<RegisteredArray>(mask->npArray));
    }
};

struct Pandas {
    static void bind(py::handle dfToBind,
        std::vector<std::unique_ptr<PandasColumnBindData>>& columnBindData,
        std::vector<common::LogicalType>& returnTypes, std::vector<std::string>& names);
};

} // namespace kuzu
