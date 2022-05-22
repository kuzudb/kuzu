#include "include/py_loader.h"

void PyLoader::initialize(py::handle& m) {
    py::class_<PyLoader>(m, "loader").def(py::init<>()).def("load", &PyLoader::load);
}

void PyLoader::load(const string& inputDir, const string& outputDir) {
    GraphLoader graphLoader(inputDir, outputDir, thread::hardware_concurrency(),
        StorageConfig::DEFAULT_BUFFER_POOL_SIZE * StorageConfig::DEFAULT_PAGES_BUFFER_RATIO,
        StorageConfig::DEFAULT_BUFFER_POOL_SIZE * StorageConfig::LARGE_PAGES_BUFFER_RATIO);
    graphLoader.loadGraph();
}
