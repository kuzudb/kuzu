#include "connector/duckdb_result_converter.h"

#include "common/type_utils.h"
#include "common/types/types.h"

namespace kuzu {
namespace duckdb_extension {

using namespace common;

template<typename T>
void convertDuckDBVectorToVector(duckdb::Vector& duckDBVector, ValueVector& result,
    uint64_t numValuesToCopy) {
    auto duckDBData = (T*)duckDBVector.GetData();
    auto validityMasks = duckdb::FlatVector::Validity(duckDBVector);
    memcpy(result.getData(), duckDBData, numValuesToCopy * result.getNumBytesPerValue());
    for (auto i = 0u; i < numValuesToCopy; i++) {
        result.setNull(i, !validityMasks.RowIsValid(i));
    }
}

template<>
void convertDuckDBVectorToVector<struct_entry_t>(duckdb::Vector& duckDBVector, ValueVector& result,
    uint64_t numValuesToCopy);
template<>
void convertDuckDBVectorToVector<list_entry_t>(duckdb::Vector& duckDBVector, ValueVector& result,
    uint64_t numValuesToCopy);

template<>
void convertDuckDBVectorToVector<ku_string_t>(duckdb::Vector& duckDBVector, ValueVector& result,
    uint64_t numValuesToCopy) {
    auto strs = reinterpret_cast<duckdb::string_t*>(duckDBVector.GetData());
    auto validityMasks = duckdb::FlatVector::Validity(duckDBVector);
    for (auto i = 0u; i < numValuesToCopy; i++) {
        result.setNull(i, !validityMasks.RowIsValid(i));
        if (!result.isNull(i)) {
            result.setValue(i, strs[i].GetString());
        }
    }
}

void DuckDBResultConverter::getDuckDBVectorConversionFunc(PhysicalTypeID physicalTypeID,
    duckdb_conversion_func_t& conversion_func) {
    TypeUtils::visit(physicalTypeID,
        [&conversion_func]<typename T>(T) { conversion_func = convertDuckDBVectorToVector<T>; });
}

template<>
void convertDuckDBVectorToVector<list_entry_t>(duckdb::Vector& duckDBVector, ValueVector& result,
    uint64_t numValuesToCopy) {
    auto numValuesInDataVec = 0u;
    auto validityMasks = duckdb::FlatVector::Validity(duckDBVector);
    switch (duckDBVector.GetType().id()) {
    case duckdb::LogicalTypeId::ARRAY: {
        auto numValuesPerList = duckdb::ArrayType::GetSize(duckDBVector.GetType());
        numValuesInDataVec = numValuesPerList * numValuesToCopy;
        for (auto i = 0u; i < numValuesToCopy; i++) {
            result.setNull(i, !validityMasks.RowIsValid(i));
            result.setValue(i, list_entry_t{numValuesPerList * i, (list_size_t)numValuesPerList});
        }
    } break;
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
        memcpy(result.getData(), duckDBVector.GetData(),
            numValuesToCopy * result.getNumBytesPerValue());
        auto listEntries = reinterpret_cast<duckdb::list_entry_t*>(duckDBVector.GetData());
        for (auto i = 0u; i < numValuesToCopy; i++) {
            result.setNull(i, !validityMasks.RowIsValid(i));
            if (!result.isNull(i)) {
                numValuesInDataVec += listEntries[i].length;
            }
        }
    } break;
    default:
        KU_UNREACHABLE;
    }

    ListVector::resizeDataVector(&result, numValuesInDataVec);
    auto dataVec = ListVector::getDataVector(&result);
    duckdb_conversion_func_t conversion_func;
    DuckDBResultConverter::getDuckDBVectorConversionFunc(dataVec->dataType.getPhysicalType(),
        conversion_func);
    conversion_func(duckdb::ListVector::GetEntry(duckDBVector), *dataVec, numValuesInDataVec);
}

template<>
void convertDuckDBVectorToVector<struct_entry_t>(duckdb::Vector& duckDBVector, ValueVector& result,
    uint64_t numValuesToCopy) {
    auto& duckdbChildrenVectors = duckdb::StructVector::GetEntries(duckDBVector);
    for (auto i = 0u; i < duckdbChildrenVectors.size(); i++) {
        duckdb_conversion_func_t conversionFunc;
        auto& duckdbChildVector = duckdbChildrenVectors[i];
        auto fieldVector = StructVector::getFieldVector(&result, i);
        DuckDBResultConverter::getDuckDBVectorConversionFunc(
            fieldVector->dataType.getPhysicalType(), conversionFunc);
        conversionFunc(*duckdbChildVector, *fieldVector, numValuesToCopy);
    }
}

DuckDBResultConverter::DuckDBResultConverter(const std::vector<LogicalType>& types) {
    conversionFunctions.resize(types.size());
    for (auto i = 0u; i < types.size(); i++) {
        getDuckDBVectorConversionFunc(types[i].getPhysicalType(), conversionFunctions[i]);
    }
}

void DuckDBResultConverter::convertDuckDBResultToVector(duckdb::DataChunk& duckDBResult,
    DataChunk& result, std::optional<std::vector<bool>> columnSkips) const {
    auto duckdbResultColIdx = 0u;
    for (auto i = 0u; i < conversionFunctions.size(); i++) {
        result.state->getSelVectorUnsafe().setSelSize(duckDBResult.size());
        if (columnSkips && columnSkips.value()[i]) {
            continue;
        }
        KU_ASSERT(duckDBResult.data[duckdbResultColIdx].GetVectorType() ==
                  duckdb::VectorType::FLAT_VECTOR);
        conversionFunctions[i](duckDBResult.data[duckdbResultColIdx],
            result.getValueVectorMutable(i), result.state->getSelVector().getSelSize());
        duckdbResultColIdx++;
    }
}

} // namespace duckdb_extension
} // namespace kuzu
