#include "cached_import/py_cached_item.h"

#include "cached_import/py_cached_import.h"
#include "common/exception/runtime.h"

namespace kuzu {

py::handle PythonCachedItem::operator()() {
    assert((bool)PyGILState_Check());
    // load if unloaded, return cached object if already loaded
    if (loaded) {
        return object;
    }
    if (parent == nullptr) {
        object = importCache->addToCache(std::move(py::module::import(name.c_str())));
    } else {
        object = importCache->addToCache(std::move((*parent)().attr(name.c_str())));
    }
    loaded = true;
    return object;
}

py::handle NumpyMaCachedItem::operator()() {
    // TODO: implement compatibility with numpy 2.0.0
    auto obj = numpy();
    if (py::cast<std::string>(obj.attr("__version__"))[0] >= '2') {
        throw common::RuntimeException(
            "Kuzu cannot currently support numpy versions at or above 2.0.0\n"
            "Try 1.26.x");
    }
    return PythonCachedItem::operator()();
}

} // namespace kuzu
