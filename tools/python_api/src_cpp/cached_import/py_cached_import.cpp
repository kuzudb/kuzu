#include "cached_import/py_cached_import.h"
#include "common/exception/runtime.h"

namespace kuzu {

std::shared_ptr<PythonCachedImport> importCache;

PythonCachedImport::~PythonCachedImport() {
    py::gil_scoped_acquire acquire;
    allObjects.clear();
}

py::handle PythonCachedImport::addToCache(py::object obj) {
    auto ptr = obj.ptr();
    allObjects.push_back(obj);
    return ptr;
}

bool doesPyModuleExist(std::string moduleName) {
    py::gil_scoped_acquire acquire;
    auto find_loader = importCache->importlib.find_loader();
    return find_loader(moduleName) != Py_None;
}

} // namespace kuzu
