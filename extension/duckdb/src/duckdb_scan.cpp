#include "duckdb_scan.h"

#include "common/exception/runtime.h"
#include "common/type_utils.h"
#include "common/types/types.h"
#include "function/table/bind_input.h"

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace duckdb_extension {

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
    return std::make_unique<DuckDBScanBindData>(query, LogicalType::copy(columnTypes), columnNames,
        initDuckDBConn);
}

DuckDBScanSharedState::DuckDBScanSharedState(
    std::unique_ptr<duckdb::MaterializedQueryResult> queryResult)
    : BaseScanSharedStateWithNumRows{queryResult->RowCount()}, queryResult{std::move(queryResult)} {
}

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
    auto result = conn.Query(scanBindData->query);
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
    common::TypeUtils::visit(physicalTypeID,
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
        result.state->getSelVectorUnsafe().setSelSize(duckDBResult.size());
        assert(duckDBResult.data[i].GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
        conversionFuncs[i](duckDBResult.data[i], *result.getValueVector(i),
            result.state->getSelVector().getSelSize());
    }
}

common::offset_t DuckDBScanFunction::tableFunc(function::TableFuncInput& input,
    function::TableFuncOutput& output) {
    auto duckdbScanSharedState = input.sharedState->ptrCast<DuckDBScanSharedState>();
    auto duckdbScanBindData = input.bindData->constPtrCast<DuckDBScanBindData>();
    std::unique_ptr<duckdb::DataChunk> result;
    try {
        // Duckdb queryResult.fetch() is not thread safe, we have to acquire a lock there.
        std::lock_guard<std::mutex> lock{duckdbScanSharedState->lock};
        result = duckdbScanSharedState->queryResult->Fetch();
    } catch (std::exception& e) {
        return 0;
    }
    if (result == nullptr) {
        return 0;
    }
    convertDuckDBResultToVector(*result, output.dataChunk, duckdbScanBindData->conversionFunctions);
    return output.dataChunk.state->getSelVector().getSelSize();
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

} // namespace duckdb_extension
} // namespace kuzu
