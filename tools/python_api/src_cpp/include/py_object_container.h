#pragma once

#include <vector>

#include "common/assert.h"
#include "pybind_include.h"

namespace kuzu {

class PythonObjectContainer {
public:
    PythonObjectContainer() {}

    ~PythonObjectContainer() {
        py::gil_scoped_acquire acquire;
        pyObjects.clear();
    }

    void push(py::object&& obj) {
        py::gil_scoped_acquire gil;
        PushInternal(std::move(obj));
    }

    const py::object& getLastAddedObject() {
        KU_ASSERT(!pyObjects.empty());
        return pyObjects.back();
    }

private:
    void PushInternal(py::object&& obj) { pyObjects.emplace_back(obj); }

private:
    std::vector<py::object> pyObjects;
};

} // namespace kuzu
