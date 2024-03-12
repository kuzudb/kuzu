#pragma once

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
    InspectCachedItem inspect;
    NumpyMaCachedItem numpyma;
    PandasCachedItem pandas;
    PyarrowCachedItem pyarrow;
    UUIDCachedItem uuid;

private:
    std::vector<py::object> allObjects;
};

extern std::shared_ptr<PythonCachedImport> importCache;

} // namespace kuzu
