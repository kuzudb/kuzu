#include "cached_import/py_cached_import.h"

namespace kuzu {

PythonCachedImport::~PythonCachedImport() {
    py::gil_scoped_acquire acquire;
    allObjects.clear();
}

py::handle PythonCachedImport::addToCache(py::object obj) {
    auto ptr = obj.ptr();
    allObjects.push_back(obj);
    return ptr;
}

std::shared_ptr<PythonCachedImport> importCache;

} // namespace kuzu
