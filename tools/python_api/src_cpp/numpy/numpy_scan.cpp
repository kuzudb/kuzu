#include "numpy/numpy_scan.h"

#include "common/types/timestamp_t.h"
#include "pandas/pandas_bind.h"

namespace kuzu {

using namespace kuzu::common;

template<class T>
void ScanNumpyColumn(
    py::array& npArray, uint64_t offset, ValueVector* outputVector, uint64_t count) {
    auto srcData = (T*)npArray.data();
    memcpy(outputVector->getData(), srcData + offset, count * sizeof(T));
}

template<class T>
void scanNumpyMasked(
    PandasColumnBindData* bindData, uint64_t count, uint64_t offset, ValueVector* outputVector) {
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
void ScanNumpyFpColumn(
    const T* srcData, uint64_t count, uint64_t offset, ValueVector* outputVector) {
    memcpy(outputVector->getData(), srcData + offset, count * sizeof(T));
    for (auto i = 0u; i < count; i++) {
        setNullIfNan(outputVector->getValue<T>(i), i, outputVector);
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
        ScanNumpyFpColumn<float>(
            reinterpret_cast<const float*>(array.data()), count, offset, outputVector);
        break;
    case NumpyNullableType::FLOAT_64:
        ScanNumpyFpColumn<double>(
            reinterpret_cast<const double*>(array.data()), count, offset, outputVector);
        break;
    case NumpyNullableType::DATETIME_NS:
    case NumpyNullableType::DATETIME_US: {
        auto sourceData = reinterpret_cast<const int64_t*>(array.data());
        auto dstData = reinterpret_cast<timestamp_t*>(outputVector->getData());
        auto timestampCastFunc = bindData->npType.type == NumpyNullableType::DATETIME_NS ?
                                     Timestamp::fromEpochNanoSeconds :
                                     Timestamp::fromEpochMicroSeconds;
        for (auto i = 0u; i < count; i++) {
            auto pos = offset + i;
            dstData[i] = timestampCastFunc(sourceData[pos]);
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
    default:
        KU_UNREACHABLE;
    }
}

} // namespace kuzu
