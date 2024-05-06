#include "py_udf.h"

#include <vector>

#include "cached_import/py_cached_import.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "function/scalar_function.h"
#include "py_connection.h"
#include "py_query_result.h"

using namespace kuzu::common;
using namespace kuzu;
using namespace kuzu::function;

struct PyUDFSignature {
    std::vector<LogicalTypeID> paramTypes;
    LogicalTypeID resultType;

    PyUDFSignature() : paramTypes(), resultType(LogicalTypeID::ANY) {}
};

static std::vector<LogicalTypeID> pyListToParams(const py::list& lst) {
    std::vector<LogicalTypeID> params;
    for (const auto& _ : lst) {
        params.push_back(LogicalTypeUtils::fromStringToID(py::cast<std::string>(_)));
    }
    return params;
}

static LogicalTypeID pyTypeToLogicalTypeID(const py::handle& ele) {
    auto datetime_val = importCache->datetime.datetime()(1, 1, 1);
    auto timedelta_val = importCache->datetime.timedelta()(0);
    auto date_val = importCache->datetime.date()(1, 1, 1);
    auto uuid_val = importCache->uuid.UUID()(py::arg("int") = 0);
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
    } else if (ele.is(py::type::of(datetime_val))) {
        return LogicalTypeID::TIMESTAMP;
    } else if (ele.is(py::type::of(date_val))) {
        return LogicalTypeID::DATE;
    } else if (ele.is(py::type::of(timedelta_val))) {
        return LogicalTypeID::INTERVAL;
    } else if (ele.is(py::type::of(uuid_val))) {
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
    PyUDFSignature UDFSignature;
    auto signature = kuzu::importCache->inspect.signature()(udf, py::arg("eval_str") = true);
    auto parameters = signature.attr("parameters");
    auto returnAnnotation = signature.attr("return_annotation");
    UDFSignature.resultType = pyTypeToLogicalTypeID(returnAnnotation);
    for (const auto& parameter : parameters) {
        auto paramAnnotation = parameters.attr("__getitem__")(parameter).attr("annotation");
        UDFSignature.paramTypes.push_back(pyTypeToLogicalTypeID(paramAnnotation));
    }
    return UDFSignature;
}

static scalar_func_exec_t getUDFExecFunc(const py::function& udf) {
    return [=](const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
               void* /* dataPtr */) -> void {
        py::gil_scoped_acquire acquire;
        result.resetAuxiliaryBuffer();
        auto& resultSelVector = result.state->getSelVector();
        for (auto i = 0u; i < resultSelVector.getSelSize(); ++i) {
            auto resultPos = resultSelVector[i];
            py::list pyParams;
            for (const auto& param : params) {
                auto paramPos =
                    param->state->isFlat() ? param->state->getSelVector()[0] : resultPos;
                auto value = param->getAsValue(paramPos);
                auto pyValue = PyQueryResult::convertValueToPyObject(*value);
                pyParams.append(pyValue);
            }
            auto pyResult = udf(*pyParams);
            auto resultValue = PyConnection::transformPythonValueAs(pyResult, result.dataType);
            result.copyFromValue(resultPos, resultValue);
        }
    };
}

function_set PyUDF::toFunctionSet(const std::string& name, const py::function& udf,
    const py::list& paramTypes, const std::string& resultType) {
    auto pySignature = analyzeSignature(udf);
    auto explicitParamTypes = pyListToParams(paramTypes);
    if (explicitParamTypes.size() > 0) {
        if (explicitParamTypes.size() != pySignature.paramTypes.size()) {
            throw RuntimeException(
                "Explicit parameters and Function parameters must be the same length");
        }
        pySignature.paramTypes = explicitParamTypes;
    }
    for (const auto& paramType : pySignature.paramTypes) {
        if (paramType == LogicalTypeID::ANY) {
            throw RuntimeException(
                "Parameters must be annotated or explicitly given, and cannot be ANY");
        }
    }
    if (resultType != "") {
        auto explicitResultType = LogicalTypeUtils::fromStringToID(resultType);
        pySignature.resultType = explicitResultType;
    }
    if (pySignature.resultType == LogicalTypeID::ANY) {
        throw RuntimeException(
            "Return value must be annotated or explicitly given, and cannot be ANY");
    }

    // TODO maxwell: support nested parameters
    for (auto paramType : pySignature.paramTypes) {
        if (LogicalTypeUtils::isNested(paramType)) {
            throw RuntimeException("Nested type UDFs not implemented yet");
        }
    }
    if (LogicalTypeUtils::isNested(pySignature.resultType)) {
        throw RuntimeException("Nested type UDFs not implemented yet");
    }

    function_set definitions;
    definitions.push_back(std::make_unique<ScalarFunction>(name, pySignature.paramTypes,
        pySignature.resultType, getUDFExecFunc(udf)));
    return definitions;
}
