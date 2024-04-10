#include "processor/operator/persistent/reader/parquet/interval_column_reader.h"

namespace kuzu {
namespace processor {

common::interval_t IntervalValueConversion::readParquetInterval(const char* input) {
    common::interval_t result;
    auto inputData = reinterpret_cast<const uint32_t*>(input);
    result.months = inputData[0];
    result.days = inputData[1];
    result.micros = int64_t(inputData[2]) * 1000;
    return result;
}

common::interval_t IntervalValueConversion::plainRead(ByteBuffer& plainData,
    ColumnReader& /*reader*/) {
    auto intervalLen = common::ParquetConstants::PARQUET_INTERVAL_SIZE;
    plainData.available(intervalLen);
    auto res = readParquetInterval(reinterpret_cast<const char*>(plainData.ptr));
    plainData.inc(intervalLen);
    return res;
}

void IntervalColumnReader::dictionary(const std::shared_ptr<ResizeableBuffer>& dictionary_data,
    uint64_t num_entries) {
    allocateDict(num_entries * sizeof(common::interval_t));
    auto dict_ptr = reinterpret_cast<common::interval_t*>(this->dict->ptr);
    for (auto i = 0u; i < num_entries; i++) {
        dict_ptr[i] = IntervalValueConversion::plainRead(*dictionary_data, *this);
    }
}

} // namespace processor
} // namespace kuzu
