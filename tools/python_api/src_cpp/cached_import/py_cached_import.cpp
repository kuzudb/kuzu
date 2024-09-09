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
    auto find_spec = importCache->importlib.util.find_spec();
#if defined(__clang__)
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#elif defined(__GNUC__)
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
    return find_spec(moduleName) != Py_None;
}

} // namespace kuzu
