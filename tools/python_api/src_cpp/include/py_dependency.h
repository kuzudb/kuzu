#include "function/table_functions/bind_data.h"
#include "pybind_include.h"

namespace kuzu {

class RegisteredObject {
public:
    explicit RegisteredObject(py::object obj) : obj{std::move(obj)} {}
    virtual ~RegisteredObject() {
        py::gil_scoped_acquire acquire;
        obj = py::none();
    }

    py::object obj;
};

class PythonDependencies : public function::ExternalDependency {
public:
    PythonDependencies()
        : ExternalDependency{function::ExternalDependenciesType::PYTHON_DEPENDENCY} {}
    explicit PythonDependencies(std::unique_ptr<RegisteredObject> pyObject)
        : ExternalDependency{function::ExternalDependenciesType::PYTHON_DEPENDENCY} {
        pyObjectList.push_back(std::move(pyObject));
    }

    std::vector<std::unique_ptr<RegisteredObject>> pyObjectList;
};

} // namespace kuzu
