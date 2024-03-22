#pragma once

#include "common/arrow/arrow_converter.h"
#include "py_object_container.h"
#include "pybind_include.h"

namespace kuzu {

namespace main {
class ClientContext;
}

struct Pyarrow {
    static std::shared_ptr<ArrowSchemaWrapper> bind(py::handle tableToBind,
        std::vector<common::LogicalType>& returnTypes, std::vector<std::string>& names);
};

} // namespace kuzu
