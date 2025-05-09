#include "py_conversion.h"

#include "cached_import/py_cached_import.h"
#include "common/case_insensitive_map.h"
#include "common/exception/not_implemented.h"
#include "common/type_utils.h"
#include "common/types/uuid.h"
#include "py_objects.h"

namespace kuzu {

using namespace kuzu::common;
using kuzu::importCache;

PythonObjectType getPythonObjectType(py::handle& ele) {
    auto pandasNa = importCache->pandas.NA();
    auto pyDateTime = importCache->datetime.datetime();
    auto pandasNat = importCache->pandas.NaT();
    auto pyDate = importCache->datetime.date();
    auto uuid = importCache->uuid.UUID();
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
    } else if (py::isinstance(ele, uuid)) {
        return PythonObjectType::UUID;
    } else if (py::isinstance<py::dict>(ele)) {
        return PythonObjectType::Dict;
    } else {
        throw NotImplementedException(stringFormat("Scanning of type {} has not been implemented",
            py::str(py::type::of(ele)).cast<std::string>()));
    }
}

void tryTransformPythonNumeric(common::ValueVector* outputVector, uint64_t pos, py::handle ele) {
    int overflow = 0;
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

static std::vector<std::string> transformStructKeys(py::handle keys, idx_t size) {
    std::vector<std::string> res;
    res.reserve(size);
    for (auto i = 0u; i < size; i++) {
        res.emplace_back(py::str(keys.attr("__getitem__")(i)));
    }
    return res;
}

void transformDictionaryToStruct(common::ValueVector* outputVector, uint64_t pos,
    const PyDictionary& dict) {
    KU_ASSERT(outputVector->dataType.getLogicalTypeID() == LogicalTypeID::STRUCT);
    auto structKeys = transformStructKeys(dict.keys, dict.len);
    if (StructType::getNumFields(outputVector->dataType) != dict.len) {
        throw common::ConversionException(
            common::stringFormat("Failed to convert python dictionary: {} to target type {}",
                dict.toString(), outputVector->dataType.toString()));
    }

    common::case_insensitive_map_t<idx_t> keyMap;
    for (idx_t i = 0; i < structKeys.size(); i++) {
        keyMap[structKeys[i]] = i;
    }

    for (auto i = 0u; i < StructType::getNumFields(outputVector->dataType); i++) {
        auto& field = StructType::getField(outputVector->dataType, i);
        auto idx = keyMap[field.getName()];
        transformPythonValue(StructVector::getFieldVector(outputVector, i).get(), pos,
            dict.values.attr("__getitem__")(idx));
    }
}

void transformDictionaryToMap(common::ValueVector* outputVector, uint64_t pos,
    const PyDictionary& dict) {
    KU_ASSERT(outputVector->dataType.getLogicalTypeID() == LogicalTypeID::MAP);
    auto keys = dict.values.attr("__getitem__")(0);
    auto values = dict.values.attr("__getitem__")(1);

    if (py::none().is(keys) || py::none().is(values)) {
        // Null map
        outputVector->setNull(pos, true /* isNull */);
    }

    auto numKeys = py::len(keys);
    KU_ASSERT(numKeys == py::len(values));
    auto listEntry = ListVector::addList(outputVector, numKeys);
    outputVector->setValue(pos, listEntry);
    auto structVector = ListVector::getDataVector(outputVector);
    auto keyVector = StructVector::getFieldVector(structVector, 0);
    auto valVector = StructVector::getFieldVector(structVector, 1);
    for (auto i = 0u; i < numKeys; i++) {
        transformPythonValue(keyVector.get(), listEntry.offset + i, keys.attr("__getitem__")(i));
        transformPythonValue(valVector.get(), listEntry.offset + i, values.attr("__getitem__")(i));
    }
}

void transformPythonValue(common::ValueVector* outputVector, uint64_t pos, py::handle ele) {
    if (ele.is_none()) {
        outputVector->setNull(pos, true /* isNull */);
        return;
    }
    outputVector->setNull(pos, false /* isNull */);
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
    case PythonObjectType::UUID: {
        outputVector->setNull(pos, false /* isNull */);
        int128_t result = 0;
        UUID::fromString(ele.attr("hex").cast<std::string>(), result);
        outputVector->setValue(pos, result);
    } break;
    case PythonObjectType::Dict: {
        PyDictionary dict = PyDictionary(py::reinterpret_borrow<py::object>(ele));
        switch (outputVector->dataType.getLogicalTypeID()) {
        case LogicalTypeID::STRUCT: {
            transformDictionaryToStruct(outputVector, pos, dict);
        } break;
        case LogicalTypeID::MAP: {
            transformDictionaryToMap(outputVector, pos, dict);
        } break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

} // namespace kuzu
