#include "numpy/numpy_scan.h"

#include "common/exception/runtime.h"
#include "common/type_utils.h"
#include "common/types/timestamp_t.h"
#include "pandas/pandas_bind.h"
#include "py_conversion.h"
#include "py_str_utils.h"
#include "utf8proc_wrapper.h"

namespace kuzu {

using namespace kuzu::common;

template<class T>
void ScanNumpyColumn(py::array& npArray, uint64_t offset, ValueVector* outputVector,
    uint64_t count) {
    auto srcData = (T*)npArray.data();
    memcpy(outputVector->getData(), srcData + offset, count * sizeof(T));
}

template<class T>
void scanNumpyMasked(PandasColumnBindData* bindData, uint64_t count, uint64_t offset,
    ValueVector* outputVector) {
    KU_ASSERT(bindData->pandasCol->getBackEnd() == PandasColumnBackend::NUMPY);
    auto& npColumn = reinterpret_cast<PandasNumpyColumn&>(*bindData->pandasCol);
    ScanNumpyColumn<T>(npColumn.array, offset, outputVector, count);
    if (bindData->mask != nullptr) {
        KU_UNREACHABLE;
    }
}

template<typename T>
void setNullIfNan(T value, uint64_t pos, ValueVector* outputVector) {
    if (std::isnan(value)) {
        outputVector->setNull(pos, true /* isNull */);
    }
}

template<class T>
void ScanNumpyFpColumn(const T* srcData, uint64_t count, uint64_t offset,
    ValueVector* outputVector) {
    memcpy(outputVector->getData(), srcData + offset, count * sizeof(T));
    for (auto i = 0u; i < count; i++) {
        setNullIfNan(outputVector->getValue<T>(i), i, outputVector);
    }
}

template<class T>
static void appendPythonUnicode(T* codepoints, uint64_t codepointLength,
    ValueVector* vectorToAppend, uint64_t pos) {
    uint64_t utf8StrLen = 0;
    for (auto i = 0u; i < codepointLength; i++) {
        auto len = utf8proc::Utf8Proc::codepointLength(int(codepoints[i]));
        KU_ASSERT(len >= 1);
        utf8StrLen += len;
    }
    auto& strToAppend = StringVector::reserveString(vectorToAppend, pos, utf8StrLen);
    // utf8proc_codepoint_to_utf8 requires that:
    // 1. codePointLen must be an int.
    // 2. dataToWrite must be a char*.
    int codePointLen = 0;
    auto dataToWrite = (char*)strToAppend.getData();
    for (auto i = 0u; i < codepointLength; i++) {
        utf8proc::Utf8Proc::codepointToUtf8(int(codepoints[i]), codePointLen, dataToWrite);
        KU_ASSERT(codePointLen >= 1);
        dataToWrite += codePointLen;
    }
    if (!ku_string_t::isShortString(utf8StrLen)) {
        memcpy(strToAppend.prefix, strToAppend.getData(), ku_string_t::PREFIX_LENGTH);
    }
}

void NumpyScan::scan(PandasColumnBindData* bindData, uint64_t count, uint64_t offset,
    common::ValueVector* outputVector) {
    KU_ASSERT(bindData->pandasCol->getBackEnd() == PandasColumnBackend::NUMPY);
    auto& npCol = reinterpret_cast<PandasNumpyColumn&>(*bindData->pandasCol);
    auto& array = npCol.array;

    switch (bindData->npType.type) {
    case NumpyNullableType::BOOL:
        scanNumpyMasked<bool>(bindData, count, offset, outputVector);
        break;
    case NumpyNullableType::UINT_8:
        scanNumpyMasked<uint8_t>(bindData, count, offset, outputVector);
        break;
    case NumpyNullableType::UINT_16:
        scanNumpyMasked<uint16_t>(bindData, count, offset, outputVector);
        break;
    case NumpyNullableType::UINT_32:
        scanNumpyMasked<uint32_t>(bindData, count, offset, outputVector);
        break;
    case NumpyNullableType::UINT_64:
        scanNumpyMasked<uint64_t>(bindData, count, offset, outputVector);
        break;
    case NumpyNullableType::INT_8:
        scanNumpyMasked<int8_t>(bindData, count, offset, outputVector);
        break;
    case NumpyNullableType::INT_16:
        scanNumpyMasked<int16_t>(bindData, count, offset, outputVector);
        break;
    case NumpyNullableType::INT_32:
        scanNumpyMasked<int32_t>(bindData, count, offset, outputVector);
        break;
    case NumpyNullableType::INT_64:
        scanNumpyMasked<int64_t>(bindData, count, offset, outputVector);
        break;
    case NumpyNullableType::FLOAT_32:
        ScanNumpyFpColumn<float>(reinterpret_cast<const float*>(array.data()), count, offset,
            outputVector);
        break;
    case NumpyNullableType::FLOAT_64:
        ScanNumpyFpColumn<double>(reinterpret_cast<const double*>(array.data()), count, offset,
            outputVector);
        break;
    case NumpyNullableType::DATETIME_S:
    case NumpyNullableType::DATETIME_MS:
    case NumpyNullableType::DATETIME_NS:
    case NumpyNullableType::DATETIME_US: {
        auto sourceData = reinterpret_cast<const int64_t*>(array.data());
        auto dstData = reinterpret_cast<timestamp_t*>(outputVector->getData());
        for (auto i = 0u; i < count; i++) {
            auto pos = offset + i;
            dstData[i].value = sourceData[pos];
            outputVector->setNull(i, false /* isNull */);
        }
        break;
    }
    case NumpyNullableType::TIMEDELTA: {
        auto sourceData = reinterpret_cast<const int64_t*>(array.data());
        auto dstData = reinterpret_cast<interval_t*>(outputVector->getData());
        for (auto i = 0u; i < count; i++) {
            auto pos = offset + i;
            auto micro = sourceData[pos] / 1000;
            auto days = micro / Interval::MICROS_PER_DAY;
            micro = micro % Interval::MICROS_PER_DAY;
            auto months = days / Interval::DAYS_PER_MONTH;
            days = days % Interval::DAYS_PER_MONTH;
            interval_t interval;
            interval.months = months;
            interval.days = days;
            interval.micros = micro;
            dstData[i] = interval;
            outputVector->setNull(i, false /* isNull */);
        }
        break;
    }
    case NumpyNullableType::OBJECT: {
        auto sourceData = (PyObject**)array.data();
        if (outputVector->dataType.getLogicalTypeID() != LogicalTypeID::STRING) {
            scanObjectColumn(sourceData, count, offset, outputVector);
            return;
        }
        auto dstData = reinterpret_cast<ku_string_t*>(outputVector->getData());
        py::gil_scoped_acquire gil;
        for (auto i = 0u; i < count; i++) {
            auto pos = i + offset;
            PyObject* val = sourceData[pos];
            if (bindData->npType.type == NumpyNullableType::OBJECT &&
                !py::isinstance<py::str>(val)) {
                if (val == Py_None ||
                    (py::isinstance<py::float_>(val) && std::isnan(PyFloat_AsDouble(val)))) {
                    outputVector->setNull(pos, true /* isNull */);
                    continue;
                }
                if (!py::isinstance<py::str>(val)) {
                    bindData->objectStrValContainer.push(std::move(py::str(val)));
                    val = reinterpret_cast<PyObject*>(
                        bindData->objectStrValContainer.getLastAddedObject().ptr());
                }
            }
            py::handle strHandle(val);
            if (!py::isinstance<py::str>(strHandle)) {
                outputVector->setNull(i, true /* isNull */);
                continue;
            }
            outputVector->setNull(i, false /* isNull */);
            if (PyStrUtil::isPyUnicodeCompatibleAscii(strHandle)) {
                dstData[i] = ku_string_t{PyStrUtil::getUnicodeStrData(strHandle),
                    PyStrUtil::getUnicodeStrLen(strHandle)};
            } else {
                auto unicodeObj = reinterpret_cast<PyCompactUnicodeObject*>(val);
                if (unicodeObj->utf8) {
                    dstData[i] = ku_string_t(reinterpret_cast<const char*>(unicodeObj->utf8),
                        unicodeObj->utf8_length);
                } else if (PyStrUtil::isPyUnicodeCompact(unicodeObj) &&
                           !PyStrUtil::isPyUnicodeASCII(unicodeObj)) {
                    auto kind = PyStrUtil::getPyUnicodeKind(strHandle);
                    switch (kind) {
                    case PyUnicode_1BYTE_KIND:
                        appendPythonUnicode<Py_UCS1>(PyStrUtil::PyUnicode1ByteData(strHandle),
                            PyStrUtil::getUnicodeStrLen(strHandle), outputVector, i);
                        break;
                    case PyUnicode_2BYTE_KIND:
                        appendPythonUnicode<Py_UCS2>(PyStrUtil::PyUnicode2ByteData(strHandle),
                            PyStrUtil::getUnicodeStrLen(strHandle), outputVector, i);
                        break;
                    case PyUnicode_4BYTE_KIND:
                        appendPythonUnicode<Py_UCS4>(PyStrUtil::PyUnicode4ByteData(strHandle),
                            PyStrUtil::getUnicodeStrLen(strHandle), outputVector, i);
                        break;
                    default:
                        KU_UNREACHABLE;
                    }
                } else {
                    // LCOV_EXCL_START
                    throw common::RuntimeException("Unsupported string format.");
                    // LCOC_EXCL_STOP
                }
            }
        }
        break;
    }
    default:
        KU_UNREACHABLE;
    }
}

void scanNumpyObject(PyObject* object, uint64_t offset, common::ValueVector* outputVector) {
    if (object == Py_None) {
        outputVector->setNull(offset, true /* isNull */);
        return;
    }
    outputVector->setNull(offset, false /* isNull */);
    transformPythonValue(outputVector, offset, object);
}

void NumpyScan::scanObjectColumn(PyObject** col, uint64_t count, uint64_t offset,
    common::ValueVector* outputVector) {
    py::gil_scoped_acquire gil;
    auto srcPtr = col + offset;
    for (auto i = 0u; i < count; i++) {
        scanNumpyObject(srcPtr[i], i, outputVector);
    }
}

} // namespace kuzu
