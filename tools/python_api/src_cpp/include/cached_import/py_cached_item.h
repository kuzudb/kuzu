#pragma once

#include <memory>
#include <string>

#include "pybind_include.h"

namespace kuzu {

class PythonCachedItem {
public:
    explicit PythonCachedItem(std::string name, PythonCachedItem* parent = nullptr)
        : name(std::move(name)), parent(parent), loaded(false) {}
    virtual ~PythonCachedItem() = default;

    bool isLoaded() const { return loaded; }
    py::handle operator()();

protected:
    std::string name;
    PythonCachedItem* parent;
    bool loaded;
    py::handle object;
};

} // namespace kuzu
