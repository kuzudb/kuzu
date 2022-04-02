#include "include/py_loader.h"

void PyLoader::initialize(py::handle& m) {
    py::class_<PyLoader>(m, "loader").def(py::init<>()).def("load", &PyLoader::load);
}

void PyLoader::load(const string& inputDir, const string& outputDir) {
    GraphLoader graphLoader(inputDir, outputDir, thread::hardware_concurrency());
    graphLoader.loadGraph();
}
