#pragma once

#include "pybind_include.h"

#include "src/loader/include/graph_loader.h"

using namespace graphflow::loader;

class PyLoader {

public:
    static void initialize(py::handle& m);

    PyLoader() = default;
    ~PyLoader() = default;

    void load(const string& inputDir, const string& outputDir);
};
