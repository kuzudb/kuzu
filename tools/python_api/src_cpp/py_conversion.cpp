#include "py_conversion.h"

#include "cached_import/py_cached_import.h"
#include "common/type_utils.h"

namespace kuzu {

using namespace kuzu::common;
using kuzu::importCache;

PythonObjectType getPythonObjectType(py::handle& ele) {
    auto pandasNa = importCache->pandas.NA();
    auto pyDateTime = importCache->datetime.datetime();
    auto pandasNat = importCache->pandas.NaT();
    auto pyDate = importCache->datetime.date();
    if (ele.is_none() || ele.is(pandasNa) || ele.is(pandasNat)) {
        return PythonObjectType::None;
    } else if (py::isinstance<py::bool_>(ele)) {
        return PythonObjectType::Bool;
    } else if (py::isinstance<py::int_>(ele)) {
        return PythonObjectType::Integer;
    } else if (py::isinstance<py::float_>(ele)) {
        return PythonObjectType::Float;
    } else if (py::isinstance(ele, pyDateTime)) {
        return PythonObjectType::Datetime;
    } else if (py::isinstance(ele, pyDate)) {
        return PythonObjectType::Date;
    } else if (py::isinstance<py::str>(ele)) {
        return PythonObjectType::String;
    } else if (py::isinstance<py::list>(ele)) {
        return PythonObjectType::List;
    } else {
        KU_UNREACHABLE;
    }
}

void tryTransformPythonNumeric(common::ValueVector* outputVector, uint64_t pos, py::handle ele) {
    int overflow;
    int64_t value = PyLong_AsLongLongAndOverflow(ele.ptr(), &overflow);
    if (overflow != 0) {
        PyErr_Clear();
        throw common::ConversionException(common::stringFormat(
            "Failed to cast value: Python value '{}' to INT64", std::string(pybind11::str(ele))));
    }
    outputVector->setNull(pos, false /* isNull */);
    outputVector->setValue(pos, value);
}

void transformListValue(common::ValueVector* outputVector, uint64_t pos, py::handle ele) {
    auto size = py::len(ele);
    auto lst = common::ListVector::addList(outputVector, size);
    outputVector->setNull(pos, false /* isNull */);
    outputVector->setValue(pos, lst);
    for (auto i = 0u; i < lst.size; i++) {
        transformPythonValue(common::ListVector::getDataVector(outputVector), lst.offset + i,
            ele.attr("__getitem__")(i));
    }
}

void transformPythonValue(common::ValueVector* outputVector, uint64_t pos, py::handle ele) {
    auto objType = getPythonObjectType(ele);
    switch (objType) {
    case PythonObjectType::None: {
        outputVector->setNull(pos, true /* isNull */);
    } break;
    case PythonObjectType::Bool: {
        outputVector->setNull(pos, false /* isNull */);
        outputVector->setValue(pos, ele.cast<bool>());
    } break;
    case PythonObjectType::Integer: {
        tryTransformPythonNumeric(outputVector, pos, ele);
    } break;
    case PythonObjectType::Float: {
        if (std::isnan(PyFloat_AsDouble(ele.ptr()))) {
            outputVector->setNull(pos, true /* isNull */);
        }
        outputVector->setNull(pos, false /* isNull */);
        outputVector->setValue(pos, ele.cast<double>());
    } break;
    case PythonObjectType::Datetime: {
        outputVector->setNull(pos, false /* isNull */);
        auto years = PyDateTime_GET_YEAR(ele.ptr());
        auto month = PyDateTime_GET_MONTH(ele.ptr());
        auto day = PyDateTime_GET_DAY(ele.ptr());
        auto hours = PyDateTime_DATE_GET_HOUR(ele.ptr());
        auto minutes = PyDateTime_DATE_GET_MINUTE(ele.ptr());
        auto seconds = PyDateTime_DATE_GET_SECOND(ele.ptr());
        auto microseconds = PyDateTime_DATE_GET_MICROSECOND(ele.ptr());
        auto date = common::Date::fromDate(years, month, day);
        auto time = common::Time::fromTime(hours, minutes, seconds, microseconds);
        outputVector->setValue(pos, common::Timestamp::fromDateTime(date, time));
    } break;
    case PythonObjectType::Date: {
        outputVector->setNull(pos, false /* isNull */);
        auto years = PyDateTime_GET_YEAR(ele.ptr());
        auto month = PyDateTime_GET_MONTH(ele.ptr());
        auto day = PyDateTime_GET_DAY(ele.ptr());
        auto dateVal = common::Date::fromDate(years, month, day);
        outputVector->setValue(pos, dateVal);
    } break;
    case PythonObjectType::String: {
        outputVector->setNull(pos, false /* isNull */);
        common::StringVector::addString(outputVector, pos, ele.cast<std::string>());
    } break;
    case PythonObjectType::List: {
        transformListValue(outputVector, pos, ele);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

} // namespace kuzu
