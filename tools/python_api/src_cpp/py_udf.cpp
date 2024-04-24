#include "py_udf.h"

#include <vector>

#include "function/scalar_function.h"
#include "cached_import/py_cached_import.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "py_connection.h"
#include "py_query_result.h"

using namespace kuzu::common;
using namespace kuzu;
using namespace kuzu::function;

struct PyUDFSignature {
    std::vector<LogicalTypeID> params;
    LogicalTypeID retval;

    PyUDFSignature(): params(), retval(LogicalTypeID::ANY) {}
};

static std::vector<LogicalTypeID> pyListToParams(const py::list& lst) {
    std::vector<LogicalTypeID> result;
    for (const auto& i: lst) {
        result.push_back(LogicalTypeUtils::fromStringToID(py::cast<std::string>(i)));
    }
    return result;
}

static LogicalTypeID pyTypeToLogicalTypeID(const py::handle& ele) {
    auto datetime_datetime = importCache->datetime.datetime();
    auto time_delta = importCache->datetime.timedelta();
    auto datetime_date = importCache->datetime.date();
    auto uuid = importCache->uuid.UUID();
    auto inspect_empty = importCache->inspect._empty();
    if (ele.is(py::type::of(py::none())) || ele.is(inspect_empty)) {
        return LogicalTypeID::ANY;
    } else if (ele.is(py::type::of(py::bool_()))) {
        return LogicalTypeID::BOOL;
    } else if (ele.is(py::type::of(py::int_()))) {
        return LogicalTypeID::INT64;
    } else if (ele.is(py::type::of(py::float_()))) {
        return LogicalTypeID::DOUBLE;
    } else if (ele.is(py::type::of(py::str()))) {
        return LogicalTypeID::STRING;
    } else if (ele.is(py::type::of(datetime_datetime(1, 1, 1)))) {
        return LogicalTypeID::TIMESTAMP;
    } else if (ele.is(py::type::of(datetime_date(1, 1, 1)))) {
        return LogicalTypeID::DATE;
    } else if (ele.is(py::type::of(time_delta(0)))) {
        return LogicalTypeID::INTERVAL;
    } else if (ele.is(py::type::of(uuid(py::arg("int") = 0)))) {
        return LogicalTypeID::UUID;
    } else if (ele.is(py::type::of(py::str()))) {
        return LogicalTypeID::STRING;
    } else if (ele.is(py::type::of(py::list()))) {
        return LogicalTypeID::LIST;
    } else if (ele.is(py::type::of(py::dict()))) {
        return LogicalTypeID::MAP;
    } else {
        throw RuntimeException(common::stringFormat("Unsupported parameter of type {}",
            py::cast<std::string>(py::str(ele))));
    }
}


static PyUDFSignature analyzeSignature(const py::function& udf) {
    PyUDFSignature retval;
    auto signature = kuzu::importCache->inspect.signature()(udf, py::arg("eval_str") = true);
    auto sig_params = signature.attr("parameters");
    auto return_annotation = signature.attr("return_annotation");
    retval.retval = pyTypeToLogicalTypeID(return_annotation);
    for (const auto& i: sig_params) {
        auto param_annotation = sig_params.attr("__getitem__")(i).attr("annotation");
        retval.params.push_back(pyTypeToLogicalTypeID(param_annotation));
    }
    return retval;
}

static scalar_func_exec_t getUDFExecution(const py::function& udf) {
    return [=](const std::vector<std::shared_ptr<ValueVector>>& params,
        ValueVector& result, void*) -> void {
        py::gil_scoped_acquire acquire;
        py::list asPyParams;
        for (const auto& param: params) {
            auto asValue = param->getAsValue(0);
            auto asPyValue = PyQueryResult::convertValueToPyObject(*asValue);
            asPyParams.append(asPyValue);
        }
        auto pyResult = udf(*asPyParams);
        auto resultValue = PyConnection::transformPythonValueAs(pyResult, &result.dataType);
        result.copyFromValue(0, resultValue);
    };
}

static scalar_func_exec_t getSelectExecution(const py::function& udf) {
    // return [=](const std::vector<stsd::shared_ptr<ValueVector>>& params, common::SelectionVector& v) {
        // TODO maxwell: implement select udf
    // };
}

function_set PyUDF::toFunctionSet(const std::string& name, const py::function& udf,
    const py::list& params, const std::string& retType) {
    auto pySignature = analyzeSignature(udf);
    auto explicitParams = pyListToParams(params);
    if (explicitParams.size() > 0) {
        if (explicitParams.size() != pySignature.params.size()) {
            throw RuntimeException("Explicit parameters and Function parameters must be the same length");
        }
        pySignature.params = explicitParams;
    }
    for (auto i: pySignature.params) {
        if (i == LogicalTypeID::ANY) {
            throw RuntimeException("Parameters must be annotated or explicitly given, and cannot be ANY");
        }
    }
    if (retType != "") {
        auto explicitRetType = LogicalTypeUtils::fromStringToID(retType);
        pySignature.retval = explicitRetType;
    }
    if (pySignature.retval == LogicalTypeID::ANY) {
        throw RuntimeException("Return value must be annotated or explicitly given, and cannot be ANY");
    }

    // TODO maxwell: support nested parameters
    for (auto i: pySignature.params) {
        if (LogicalTypeUtils::isNested(i)) {
            throw RuntimeException("Nested type UDFs not implemented yet");
        }
    }
    if (LogicalTypeUtils::isNested(pySignature.retval)) {
        throw RuntimeException("Nested type UDFs not implemented yet");
    }

    function_set definitions;
    definitions.push_back(std::make_unique<ScalarFunction>(name, pySignature.params,
        pySignature.retval, getUDFExecution(udf)));
    return definitions;
}
