#pragma once

#include "common/types/types.h"
#include "pybind_include.h"

namespace kuzu {

struct PyDictionary {
public:
    explicit PyDictionary(py::object dict)
        : keys{py::list(dict.attr("keys")())}, values{py::list(dict.attr("values")())},
          len{static_cast<common::idx_t>(py::len(keys))}, dict{std::move(dict)} {}

    // These are cached so we don't have to create new objects all the time
    // The CPython API offers PyDict_Keys but that creates a new reference every time, same for
    // values
    py::object keys;
    py::object values;
    common::idx_t len;

public:
    py::handle operator[](const py::object& obj) const {
        return PyDict_GetItem(dict.ptr(), obj.ptr());
    }

    std::string toString() const { return std::string(py::str(dict)); }

private:
    py::object dict;
};

} // namespace kuzu
