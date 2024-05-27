#include "py_udf.h"

#include <vector>

#include "cached_import/py_cached_import.h"
#include "common/exception/not_implemented.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "function/scalar_function.h"
#include "py_connection.h"
#include "py_query_result.h"

using namespace kuzu::common;
using namespace kuzu;
using namespace kuzu::function;

struct PyUDFSignature {
    std::vector<LogicalType> paramTypes;
    LogicalType resultType;

    PyUDFSignature() : paramTypes(), resultType(LogicalTypeID::ANY) {}
};

static std::vector<LogicalType> pyListToParams(const py::list& lst) {
    std::vector<LogicalType> params;
    for (const auto& _ : lst) {
        params.push_back(LogicalType::fromString(py::cast<std::string>(_)));
    }
    return params;
}

static LogicalType getLogicalType(const py::handle& ele);

static LogicalType getLogicalTypeNested(const py::handle& ele) {
    auto datetime_val = importCache->datetime.datetime()(1, 1, 1);
    auto timedelta_val = importCache->datetime.timedelta()(0);
    auto date_val = importCache->datetime.date()(1, 1, 1);
    auto uuid_val = importCache->uuid.UUID()(py::arg("int") = 0);
    py::object origin = ele.attr("__origin__");
    if (origin.is(py::type::of(py::list()))) {
        auto args = ele.attr("__args__");
        if (py::len(args) != 1) {
            throw RuntimeException("List annotations must have exactly one type argument");
        }
        auto getitem = args.attr("__getitem__");
        return *LogicalType::LIST(getLogicalType(getitem(0)));
    } else if (origin.is(py::type::of(py::dict()))) {
        auto args = ele.attr("__args__");
        if (py::len(args) != 2) {
            throw RuntimeException("Map annotations must have exactly two type arguments");
        }
        auto getitem = args.attr("__getitem__");
        return *LogicalType::MAP(getLogicalType(getitem(0)), getLogicalType(getitem(1)));
    } else {
        throw NotImplementedException(
            "Currently Python UDFs only support LIST and MAP return types");
    }
}

static LogicalType getLogicalTypeNonNested(const py::handle& ele) {
    auto datetime_val = importCache->datetime.datetime()(1, 1, 1);
    auto timedelta_val = importCache->datetime.timedelta()(0);
    auto date_val = importCache->datetime.date()(1, 1, 1);
    auto uuid_val = importCache->uuid.UUID()(py::arg("int") = 0);
    auto inspect_empty = importCache->inspect._empty();
    if (ele.is(py::type::of(py::none())) || ele.is(inspect_empty)) {
        return *LogicalType::ANY();
    } else if (ele.is(py::type::of(py::bool_()))) {
        return *LogicalType::BOOL();
    } else if (ele.is(py::type::of(py::int_()))) {
        return *LogicalType::INT64();
    } else if (ele.is(py::type::of(py::float_()))) {
        return *LogicalType::DOUBLE();
    } else if (ele.is(py::type::of(py::str()))) {
        return *LogicalType::STRING();
    } else if (ele.is(py::type::of(datetime_val))) {
        return *LogicalType::TIMESTAMP();
    } else if (ele.is(py::type::of(date_val))) {
        return *LogicalType::DATE();
    } else if (ele.is(py::type::of(timedelta_val))) {
        return *LogicalType::INTERVAL();
    } else if (ele.is(py::type::of(uuid_val))) {
        return *LogicalType::UUID();
    } else if (ele.is(py::type::of(py::list()))) {
        throw RuntimeException("List annotations must specify child type");
    } else if (ele.is(py::type::of(py::dict()))) {
        throw RuntimeException("Map annotations must specify child types");
    } else {
        throw RuntimeException(common::stringFormat("Unsupported annotation of type {}",
            py::cast<std::string>(py::str(ele))));
    }
}

static LogicalType getLogicalType(const py::handle& ele) {
    if (py::hasattr(ele, "__origin__")) {
        return getLogicalTypeNested(ele);
    } else {
        return getLogicalTypeNonNested(ele);
    }
}

static py::object getSignature(const py::function& udf) {
    constexpr int32_t PYTHON_3_10_HEX = 0x030a00f0;
    auto python_version = PY_VERSION_HEX;

    auto signature_func = kuzu::importCache->inspect.signature();
    if (python_version >= PYTHON_3_10_HEX) {
        return signature_func(udf, py::arg("eval_str") = true);
    } else {
        return signature_func(udf);
    }
}

static PyUDFSignature analyzeSignature(const py::function& udf) {
    PyUDFSignature UDFSignature;
    auto signature = getSignature(udf);
    auto parameters = signature.attr("parameters");
    auto returnAnnotation = signature.attr("return_annotation");
    UDFSignature.resultType = getLogicalType(returnAnnotation);
    for (const auto& parameter : parameters) {
        auto paramAnnotation = parameters.attr("__getitem__")(parameter).attr("annotation");
        UDFSignature.paramTypes.push_back(getLogicalType(paramAnnotation));
    }
    return UDFSignature;
}

static scalar_func_exec_t getUDFExecFunc(const py::function& udf, bool defaultNull,
    bool catchExceptions) {
    return [=](const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
               void* /* dataPtr */) -> void {
        py::gil_scoped_acquire acquire;
        result.resetAuxiliaryBuffer();
        auto& resultSelVector = result.state->getSelVector();
        for (auto i = 0u; i < resultSelVector.getSelSize(); ++i) {
            auto resultPos = resultSelVector[i];
            py::list pyParams;
            bool hasNull = false;
            for (const auto& param : params) {
                auto paramPos =
                    param->state->isFlat() ? param->state->getSelVector()[0] : resultPos;
                auto value = param->getAsValue(paramPos);
                if (value->isNull()) {
                    hasNull = true;
                }
                auto pyValue = PyQueryResult::convertValueToPyObject(*value);
                pyParams.append(pyValue);
            }
            if (defaultNull && hasNull) {
                result.setNull(resultPos, true);
            } else {
                try {
                    auto pyResult = udf(*pyParams);
                    auto resultValue =
                        PyConnection::transformPythonValueAs(pyResult, result.dataType);
                    result.copyFromValue(resultPos, resultValue);
                } catch (py::error_already_set& e) {
                    if (catchExceptions) {
                        result.setNull(resultPos, true);
                    } else {
                        throw common::RuntimeException(e.what());
                    }
                }
            }
        }
    };
}

static scalar_bind_func getUDFBindFunc(const PyUDFSignature& signature) {
    return [=](const binder::expression_vector&, Function*) -> std::unique_ptr<FunctionBindData> {
        return std::make_unique<FunctionBindData>(signature.paramTypes,
            std::make_unique<LogicalType>(signature.resultType));
    };
}

function_set PyUDF::toFunctionSet(const std::string& name, const py::function& udf,
    const py::list& paramTypes, const std::string& resultType, bool defaultNull,
    bool catchExceptions) {
    auto pySignature = analyzeSignature(udf);
    auto explicitParamTypes = pyListToParams(paramTypes);
    if (explicitParamTypes.size() > 0) {
        if (explicitParamTypes.size() != pySignature.paramTypes.size()) {
            throw RuntimeException(
                "Explicit parameters and Function parameters must be the same length");
        }
        pySignature.paramTypes = explicitParamTypes;
    }
    std::vector<LogicalTypeID> paramIDTypes;
    for (const auto& paramType : pySignature.paramTypes) {
        if (paramType.getLogicalTypeID() == LogicalTypeID::ANY) {
            throw RuntimeException(
                "Parameters must be annotated or explicitly given, and cannot be ANY");
        }
        paramIDTypes.push_back(paramType.getLogicalTypeID());
    }
    if (resultType != "") {
        pySignature.resultType = LogicalType::fromString(resultType);
    }
    if (pySignature.resultType.getLogicalTypeID() == LogicalTypeID::ANY) {
        throw RuntimeException(
            "Return value must be annotated or explicitly given, and cannot be ANY");
    }

    function_set definitions;
    definitions.push_back(std::make_unique<ScalarFunction>(name, paramIDTypes,
        pySignature.resultType.getLogicalTypeID(),
        getUDFExecFunc(udf, defaultNull, catchExceptions), getUDFBindFunc(pySignature)));
    return definitions;
}
