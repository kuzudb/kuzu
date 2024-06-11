#pragma once

#include <string>
#include <vector>

#include "py_cached_modules.h"

namespace kuzu {

class PythonCachedImport {
public:
    // Note: Callers generally acquire the GIL prior to entering functions
    // that require the import cache.

    PythonCachedImport() = default;
    ~PythonCachedImport();

    py::handle addToCache(py::object obj);

    DateTimeCachedItem datetime;
    DecimalCachedItem decimal;
    ImportLibCachedItem importlib;
    InspectCachedItem inspect;
    NumpyMaCachedItem numpyma;
    PandasCachedItem pandas;
    PolarsCachedItem polars;
    PyarrowCachedItem pyarrow;
    UUIDCachedItem uuid;

private:
    std::vector<py::object> allObjects;
};

bool doesPyModuleExist(std::string moduleName);

extern std::shared_ptr<PythonCachedImport> importCache;

} // namespace kuzu
