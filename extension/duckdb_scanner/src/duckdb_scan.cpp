#include "duckdb_scan.h"

#include "common/types/types.h"
#include "function/table/bind_input.h"

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace duckdb_scanner {

void getDuckDBVectorConversionFunc(PhysicalTypeID physicalTypeID,
    duckdb_conversion_func_t& conversion_func);

DuckDBScanBindData::DuckDBScanBindData(std::string query,
    std::vector<common::LogicalType> columnTypes, std::vector<std::string> columnNames,
    init_duckdb_conn_t initDuckDBConn)
    : TableFuncBindData{std::move(columnTypes), std::move(columnNames)}, query{std::move(query)},
      initDuckDBConn{std::move(initDuckDBConn)} {
    conversionFunctions.resize(this->columnTypes.size());
    for (auto i = 0u; i < this->columnTypes.size(); i++) {
        getDuckDBVectorConversionFunc(this->columnTypes[i].getPhysicalType(),
            conversionFunctions[i]);
    }
}

std::unique_ptr<TableFuncBindData> DuckDBScanBindData::copy() const {
    return std::make_unique<DuckDBScanBindData>(query, columnTypes, columnNames, initDuckDBConn);
}

DuckDBScanSharedState::DuckDBScanSharedState(std::unique_ptr<duckdb::QueryResult> queryResult)
    : TableFuncSharedState(), queryResult{std::move(queryResult)} {}

struct DuckDBScanFunction {
    static constexpr char DUCKDB_SCAN_FUNC_NAME[] = "duckdb_scan";

    static common::offset_t tableFunc(function::TableFuncInput& input,
        function::TableFuncOutput& output);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(DuckDBScanBindData bindData,
        main::ClientContext* /*context*/, function::TableFuncBindInput* input);

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);

    static std::unique_ptr<function::TableFuncLocalState> initLocalState(
        function::TableFunctionInitInput& input, function::TableFuncSharedState* state,
        storage::MemoryManager*
        /*mm*/);
};

std::unique_ptr<function::TableFuncSharedState> DuckDBScanFunction::initSharedState(
    function::TableFunctionInitInput& input) {
    auto scanBindData = reinterpret_cast<DuckDBScanBindData*>(input.bindData);
    auto [db, conn] = scanBindData->initDuckDBConn();
    auto result = conn.SendQuery(scanBindData->query);
    if (result->HasError()) {
        throw common::RuntimeException(
            common::stringFormat("Failed to execute query: {} in duckdb.", result->GetError()));
    }
    return std::make_unique<DuckDBScanSharedState>(std::move(result));
}

std::unique_ptr<function::TableFuncLocalState> DuckDBScanFunction::initLocalState(
    function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* /*state*/,
    storage::MemoryManager* /*mm*/) {
    return std::make_unique<function::TableFuncLocalState>();
}

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

void getDuckDBVectorConversionFunc(PhysicalTypeID physicalTypeID,
    duckdb_conversion_func_t& conversion_func) {
    switch (physicalTypeID) {
    case PhysicalTypeID::BOOL: {
        conversion_func = convertDuckDBVectorToVector<bool>;
    } break;
    case PhysicalTypeID::INT128: {
        conversion_func = convertDuckDBVectorToVector<int128_t>;
    } break;
    case PhysicalTypeID::INT64: {
        conversion_func = convertDuckDBVectorToVector<int64_t>;
    } break;
    case PhysicalTypeID::INT32: {
        conversion_func = convertDuckDBVectorToVector<int32_t>;
    } break;
    case PhysicalTypeID::INT16: {
        conversion_func = convertDuckDBVectorToVector<int16_t>;
    } break;
    case PhysicalTypeID::INT8: {
        conversion_func = convertDuckDBVectorToVector<int8_t>;
    } break;
    case PhysicalTypeID::UINT64: {
        conversion_func = convertDuckDBVectorToVector<uint64_t>;
    } break;
    case PhysicalTypeID::UINT32: {
        conversion_func = convertDuckDBVectorToVector<uint32_t>;
    } break;
    case PhysicalTypeID::UINT16: {
        conversion_func = convertDuckDBVectorToVector<uint16_t>;
    } break;
    case PhysicalTypeID::UINT8: {
        conversion_func = convertDuckDBVectorToVector<uint8_t>;
    } break;
    case PhysicalTypeID::DOUBLE: {
        conversion_func = convertDuckDBVectorToVector<double>;
    } break;
    case PhysicalTypeID::FLOAT: {
        conversion_func = convertDuckDBVectorToVector<float>;
    } break;
    case PhysicalTypeID::STRING: {
        conversion_func = convertDuckDBVectorToVector<ku_string_t>;
    } break;
    case PhysicalTypeID::INTERVAL: {
        conversion_func = convertDuckDBVectorToVector<interval_t>;
    } break;
    case PhysicalTypeID::LIST: {
        conversion_func = convertDuckDBVectorToVector<list_entry_t>;
    } break;
    case PhysicalTypeID::STRUCT: {
        conversion_func = convertDuckDBVectorToVector<struct_entry_t>;
    } break;
    default:
        KU_UNREACHABLE;
    }
}

template<>
void convertDuckDBVectorToVector<list_entry_t>(duckdb::Vector& duckDBVector, ValueVector& result,
    uint64_t numValuesToCopy) {
    memcpy(result.getData(), duckDBVector.GetData(),
        numValuesToCopy * result.getNumBytesPerValue());
    auto numValuesInDataVec = 0;
    auto listEntries = reinterpret_cast<duckdb::list_entry_t*>(duckDBVector.GetData());
    auto validityMasks = duckdb::FlatVector::Validity(duckDBVector);
    for (auto i = 0u; i < numValuesToCopy; i++) {
        result.setNull(i, !validityMasks.RowIsValid(i));
        if (!result.isNull(i)) {
            numValuesInDataVec += listEntries[i].length;
        }
    }
    ListVector::resizeDataVector(&result, numValuesInDataVec);
    auto dataVec = ListVector::getDataVector(&result);
    duckdb_conversion_func_t conversion_func;
    getDuckDBVectorConversionFunc(dataVec->dataType.getPhysicalType(), conversion_func);
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
        getDuckDBVectorConversionFunc(fieldVector->dataType.getPhysicalType(), conversionFunc);
        conversionFunc(*duckdbChildVector, *fieldVector, numValuesToCopy);
    }
}

static void convertDuckDBResultToVector(duckdb::DataChunk& duckDBResult, DataChunk& result,
    std::vector<duckdb_conversion_func_t> conversionFuncs) {
    for (auto i = 0u; i < conversionFuncs.size(); i++) {
        result.state->selVector->selectedSize = duckDBResult.size();
        assert(duckDBResult.data[i].GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
        conversionFuncs[i](duckDBResult.data[i], *result.getValueVector(i),
            result.state->selVector->selectedSize);
    }
}

common::offset_t DuckDBScanFunction::tableFunc(function::TableFuncInput& input,
    function::TableFuncOutput& output) {
    auto duckdbScanSharedState = reinterpret_cast<DuckDBScanSharedState*>(input.sharedState);
    auto duckdbScanBindData = reinterpret_cast<DuckDBScanBindData*>(input.bindData);
    std::unique_ptr<duckdb::DataChunk> result;
    try {
        result = duckdbScanSharedState->queryResult->Fetch();
    } catch (std::exception& e) {
        return 0;
    }
    if (result == nullptr) {
        return 0;
    }
    convertDuckDBResultToVector(*result, output.dataChunk, duckdbScanBindData->conversionFunctions);
    return output.dataChunk.state->selVector->selectedSize;
}

std::unique_ptr<function::TableFuncBindData> DuckDBScanFunction::bindFunc(
    DuckDBScanBindData bindData, main::ClientContext* /*clientContext*/,
    function::TableFuncBindInput* /*input*/) {
    return bindData.copy();
}

TableFunction getScanFunction(DuckDBScanBindData bindData) {
    return TableFunction(DuckDBScanFunction::DUCKDB_SCAN_FUNC_NAME, DuckDBScanFunction::tableFunc,
        std::bind(DuckDBScanFunction::bindFunc, std::move(bindData), std::placeholders::_1,
            std::placeholders::_2),
        DuckDBScanFunction::initSharedState, DuckDBScanFunction::initLocalState,
        std::vector<LogicalTypeID>{});
}

} // namespace duckdb_scanner
} // namespace kuzu
