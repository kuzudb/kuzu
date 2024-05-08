#include "include/py_connection.h"

#include <utility>

#include "cached_import/py_cached_import.h"
#include "common/constants.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "common/types/uuid.h"
#include "common/utils.h"
#include "datetime.h" // from Python
#include "function/built_in_function_utils.h"
#include "include/py_udf.h"
#include "main/connection.h"
#include "pandas/pandas_scan.h"
#include "processor/result/factorized_table.h"
#include "pyarrow/pyarrow_scan.h"

using namespace kuzu::common;
using namespace kuzu;

void PyConnection::initialize(py::handle& m) {
    py::class_<PyConnection>(m, "Connection")
        .def(py::init<PyDatabase*, uint64_t>(), py::arg("database"), py::arg("num_threads") = 0)
        .def("close", &PyConnection::close)
        .def("execute", &PyConnection::execute, py::arg("prepared_statement"),
            py::arg("parameters") = py::dict())
        .def("query", &PyConnection::query, py::arg("statement"))
        .def("set_max_threads_for_exec", &PyConnection::setMaxNumThreadForExec,
            py::arg("num_threads"))
        .def("prepare", &PyConnection::prepare, py::arg("query"))
        .def("set_query_timeout", &PyConnection::setQueryTimeout, py::arg("timeout_in_ms"))
        .def("get_num_nodes", &PyConnection::getNumNodes, py::arg("node_name"))
        .def("get_num_rels", &PyConnection::getNumRels, py::arg("rel_name"))
        .def("get_all_edges_for_torch_geometric", &PyConnection::getAllEdgesForTorchGeometric,
            py::arg("np_array"), py::arg("src_table_name"), py::arg("rel_name"),
            py::arg("dst_table_name"), py::arg("query_batch_size"))
        .def("create_function", &PyConnection::createScalarFunction, py::arg("name"),
            py::arg("udf"), py::arg("params_type"), py::arg("return_value"));
    PyDateTime_IMPORT;
}

static std::unique_ptr<function::ScanReplacementData> tryReplacePolars(py::dict& dict,
    py::str& objectName) {
    if (!dict.contains(objectName)) {
        return nullptr;
    }
    auto entry = dict[objectName];
    if (py::isinstance(entry, importCache->polars.DataFrame())) {
        auto scanReplacementData = std::make_unique<function::ScanReplacementData>();
        scanReplacementData->func = PyArrowTableScanFunction::getFunction();
        auto bindInput = function::TableFuncBindInput();
        bindInput.inputs.push_back(Value::createValue(reinterpret_cast<uint8_t*>(entry.ptr())));
        scanReplacementData->bindInput = std::move(bindInput);
        return scanReplacementData;
    } else {
        return nullptr;
    }
}

static std::unique_ptr<function::ScanReplacementData> replacePythonObject(
    const std::string& objectName) {
    py::gil_scoped_acquire acquire;
    auto pyTableName = py::str(objectName);
    // Here we do an exhaustive search on the frame lineage.
    auto currentFrame = importCache->inspect.currentframe()();
    bool nameMatchFound = false;
    while (hasattr(currentFrame, "f_locals")) {
        auto localDict = py::reinterpret_borrow<py::dict>(currentFrame.attr("f_locals"));
        if (localDict) {
            if (localDict.contains(pyTableName)) {
                nameMatchFound = true;
            }
            auto result = tryReplacePD(localDict, pyTableName);
            if (!result) {
                result = tryReplacePolars(localDict, pyTableName);
            }
            if (result) {
                return result;
            }
        }
        auto globalDict = py::reinterpret_borrow<py::dict>(currentFrame.attr("f_globals"));
        if (globalDict) {
            if (globalDict.contains(pyTableName)) {
                nameMatchFound = true;
            }
            auto result = tryReplacePD(globalDict, pyTableName);
            if (!result) {
                result = tryReplacePolars(localDict, pyTableName);
            }
            if (result) {
                return result;
            }
        }
        currentFrame = currentFrame.attr("f_back");
    }
    if (nameMatchFound) {
        throw BinderException(
            stringFormat("Variable {} found but no matches were DataFrames", objectName));
    }
    return nullptr;
}

PyConnection::PyConnection(PyDatabase* pyDatabase, uint64_t numThreads) {
    storageDriver = std::make_unique<kuzu::main::StorageDriver>(pyDatabase->database.get());
    conn = std::make_unique<Connection>(pyDatabase->database.get());
    conn->getClientContext()->addScanReplace(function::ScanReplacement(replacePythonObject));
    if (numThreads > 0) {
        conn->setMaxNumThreadForExec(numThreads);
    }
}

void PyConnection::close() {
    conn.reset();
}

void PyConnection::setQueryTimeout(uint64_t timeoutInMS) {
    conn->setQueryTimeOut(timeoutInMS);
}

static std::unordered_map<std::string, std::unique_ptr<Value>> transformPythonParameters(
    const py::dict& params, Connection* conn);

std::unique_ptr<PyQueryResult> PyConnection::execute(PyPreparedStatement* preparedStatement,
    const py::dict& params) {
    auto parameters = transformPythonParameters(params, conn.get());
    py::gil_scoped_release release;
    auto queryResult =
        conn->executeWithParams(preparedStatement->preparedStatement.get(), std::move(parameters));
    py::gil_scoped_acquire acquire;
    return checkAndWrapQueryResult(queryResult);
}

std::unique_ptr<PyQueryResult> PyConnection::query(const std::string& statement) {
    py::gil_scoped_release release;
    auto queryResult = conn->query(statement);
    py::gil_scoped_acquire acquire;
    return checkAndWrapQueryResult(queryResult);
}

void PyConnection::setMaxNumThreadForExec(uint64_t numThreads) {
    conn->setMaxNumThreadForExec(numThreads);
}

PyPreparedStatement PyConnection::prepare(const std::string& query) {
    auto preparedStatement = conn->prepare(query);
    PyPreparedStatement pyPreparedStatement;
    pyPreparedStatement.preparedStatement = std::move(preparedStatement);
    return pyPreparedStatement;
}

uint64_t PyConnection::getNumNodes(const std::string& nodeName) {
    return storageDriver->getNumNodes(nodeName);
}

uint64_t PyConnection::getNumRels(const std::string& relName) {
    return storageDriver->getNumRels(relName);
}

void PyConnection::getAllEdgesForTorchGeometric(py::array_t<int64_t>& npArray,
    const std::string& srcTableName, const std::string& relName, const std::string& dstTableName,
    size_t queryBatchSize) {
    // Get the number of nodes in the dst table for batching.
    auto numDstNodes = getNumNodes(dstTableName);
    uint64_t batches = numDstNodes / queryBatchSize;
    if (numDstNodes % queryBatchSize != 0) {
        batches += 1;
    }
    auto numRels = getNumRels(relName);

    auto bufferInfo = npArray.request();
    auto buffer = (int64_t*)bufferInfo.ptr;

    // Set the number of threads to 1 for fetching edges to ensure ordering.
    auto numThreadsForExec = conn->getMaxNumThreadForExec();
    conn->setMaxNumThreadForExec(1);
    // Run queries in batch to fetch edges.
    auto queryString = "MATCH (a:{})-[:{}]->(b:{}) WHERE offset(id(b)) >= $s AND offset(id(b)) < "
                       "$e RETURN offset(id(a)), offset(id(b))";
    auto query = stringFormat(queryString, srcTableName, relName, dstTableName);
    auto preparedStatement = conn->prepare(query);
    auto srcBuffer = buffer;
    auto dstBuffer = buffer + numRels;
    for (uint64_t batch = 0; batch < batches; ++batch) {
        // Must be int64_t for parameter typing.
        int64_t start = batch * queryBatchSize;
        int64_t end = (batch + 1) * queryBatchSize;
        end = (uint64_t)end > numDstNodes ? numDstNodes : end;
        std::unordered_map<std::string, std::unique_ptr<Value>> parameters;
        parameters["s"] = std::make_unique<Value>(start);
        parameters["e"] = std::make_unique<Value>(end);
        auto result = conn->executeWithParams(preparedStatement.get(), std::move(parameters));
        if (!result->isSuccess()) {
            throw std::runtime_error(result->getErrorMessage());
        }
        auto table = result->getTable();
        auto tableSchema = table->getTableSchema();
        if (tableSchema->getColumn(0)->isFlat() && !tableSchema->getColumn(1)->isFlat()) {
            for (auto i = 0u; i < table->getNumTuples(); ++i) {
                auto tuple = table->getTuple(i);
                auto overflowValue = (overflow_value_t*)(tuple + tableSchema->getColOffset(1));
                for (auto j = 0u; j < overflowValue->numElements; ++j) {
                    srcBuffer[j] = *(int64_t*)(tuple + tableSchema->getColOffset(0));
                }
                for (auto j = 0u; j < overflowValue->numElements; ++j) {
                    dstBuffer[j] = ((int64_t*)overflowValue->value)[j];
                }
                srcBuffer += overflowValue->numElements;
                dstBuffer += overflowValue->numElements;
            }
        } else if (tableSchema->getColumn(1)->isFlat() && !tableSchema->getColumn(0)->isFlat()) {
            for (auto i = 0u; i < table->getNumTuples(); ++i) {
                auto tuple = table->getTuple(i);
                auto overflowValue = (overflow_value_t*)(tuple + tableSchema->getColOffset(0));
                for (auto j = 0u; j < overflowValue->numElements; ++j) {
                    srcBuffer[j] = ((int64_t*)overflowValue->value)[j];
                }
                for (auto j = 0u; j < overflowValue->numElements; ++j) {
                    dstBuffer[j] = *(int64_t*)(tuple + tableSchema->getColOffset(1));
                }
                srcBuffer += overflowValue->numElements;
                dstBuffer += overflowValue->numElements;
            }
        } else {
            throw std::runtime_error("Wrong result table schema.");
        }
    }
    conn->setMaxNumThreadForExec(numThreadsForExec);
}

bool PyConnection::isPandasDataframe(const py::object& object) {
    return py::isinstance(object, importCache->pandas.DataFrame());
}

static std::unordered_map<std::string, std::unique_ptr<Value>> transformPythonParameters(
    const py::dict& params, Connection* conn) {
    std::unordered_map<std::string, std::unique_ptr<Value>> result;
    for (auto& [key, value] : params) {
        if (!py::isinstance<py::str>(key)) {
            // TODO(Chang): remove ROLLBACK once we can guarantee database is deleted after conn
            conn->query("ROLLBACK");
            throw std::runtime_error("Parameter name must be of type string but got " +
                                     py::str(key.get_type()).cast<std::string>());
        }
        auto name = key.cast<std::string>();
        auto val = std::make_unique<Value>(PyConnection::transformPythonValue(value));
        result.insert({name, std::move(val)});
    }
    return result;
}

static std::unique_ptr<LogicalType> pyLogicalType(const py::handle& val) {
    auto datetime_datetime = importCache->datetime.datetime();
    auto time_delta = importCache->datetime.timedelta();
    auto datetime_date = importCache->datetime.date();
    auto uuid = importCache->uuid.UUID();
    if (val.is_none()) {
        return LogicalType::ANY();
    } else if (py::isinstance<py::bool_>(val)) {
        return LogicalType::BOOL();
    } else if (py::isinstance<py::int_>(val)) {
        auto nativeValue = val.cast<int64_t>();
        if (integerFitsIn<int8_t>(nativeValue)) {
            return LogicalType::INT8();
        } else if (integerFitsIn<uint8_t>(nativeValue)) {
            return LogicalType::UINT8();
        } else if (integerFitsIn<int16_t>(nativeValue)) {
            return LogicalType::INT16();
        } else if (integerFitsIn<uint16_t>(nativeValue)) {
            return LogicalType::UINT16();
        } else if (integerFitsIn<int32_t>(nativeValue)) {
            return LogicalType::INT32();
        } else if (integerFitsIn<uint32_t>(nativeValue)) {
            return LogicalType::UINT32();
        } else {
            return LogicalType::INT64();
        }
    } else if (py::isinstance<py::float_>(val)) {
        return LogicalType::DOUBLE();
    } else if (py::isinstance<py::str>(val)) {
        return LogicalType::STRING();
    } else if (py::isinstance(val, datetime_datetime)) {
        return LogicalType::TIMESTAMP();
    } else if (py::isinstance(val, datetime_date)) {
        return LogicalType::DATE();
    } else if (py::isinstance(val, time_delta)) {
        return LogicalType::INTERVAL();
    } else if (py::isinstance(val, uuid)) {
        return LogicalType::UUID();
    } else if (py::isinstance<py::list>(val)) {
        py::list lst = py::reinterpret_borrow<py::list>(val);
        auto childType = LogicalType::ANY();
        for (auto child : lst) {
            auto curChildType = pyLogicalType(child);
            LogicalType result;
            if (!LogicalTypeUtils::tryGetMaxLogicalType(*childType, *curChildType, result)) {
                throw RuntimeException(stringFormat(
                    "Cannot convert Python object to Kuzu value : {}  is incompatible with {}",
                    childType->toString(), curChildType->toString()));
            }
            childType = std::make_unique<LogicalType>(result);
        }
        return LogicalType::LIST(std::move(childType));
    } else if (py::isinstance<py::dict>(val)) {
        py::dict dict = py::reinterpret_borrow<py::dict>(val);
        auto childKeyType = LogicalType::ANY(), childValueType = LogicalType::ANY();
        for (auto child : dict) {
            auto curChildKeyType = pyLogicalType(child.first),
                 curChildValueType = pyLogicalType(child.second);
            LogicalType resultKey, resultValue;
            if (!LogicalTypeUtils::tryGetMaxLogicalType(*childKeyType, *curChildKeyType,
                    resultKey)) {
                throw RuntimeException(stringFormat(
                    "Cannot convert Python object to Kuzu value : {}  is incompatible with {}",
                    childKeyType->toString(), curChildKeyType->toString()));
            }
            if (!LogicalTypeUtils::tryGetMaxLogicalType(*childValueType, *curChildValueType,
                    resultValue)) {
                throw RuntimeException(stringFormat(
                    "Cannot convert Python object to Kuzu value : {}  is incompatible with {}",
                    childValueType->toString(), curChildValueType->toString()));
            }
            childKeyType = std::make_unique<LogicalType>(resultKey);
            childValueType = std::make_unique<LogicalType>(resultValue);
        }
        return LogicalType::MAP(std::move(childKeyType), std::move(childValueType));
    } else {
        // LCOV_EXCL_START
        throw common::RuntimeException(
            "Unknown parameter type " + py::str(val.get_type()).cast<std::string>());
        // LCOV_EXCL_STOP
    }
}

Value PyConnection::transformPythonValueAs(const py::handle& val, const LogicalType& type) {
    // ignore the type of the actual python object, just directly cast
    auto datetime_datetime = importCache->datetime.datetime();
    auto time_delta = importCache->datetime.timedelta();
    auto datetime_date = importCache->datetime.date();
    if (val.is_none()) {
        return Value::createNullValue(type);
    }
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::ANY:
        return Value::createNullValue();
    case LogicalTypeID::BOOL:
        return Value::createValue<bool>(py::cast<py::bool_>(val).cast<bool>());
    case LogicalTypeID::INT64:
        return Value::createValue<int64_t>(py::cast<py::int_>(val).cast<int64_t>());
    case LogicalTypeID::UINT32:
        return Value::createValue<uint32_t>(py::cast<py::int_>(val).cast<uint32_t>());
    case LogicalTypeID::INT32:
        return Value::createValue<int32_t>(py::cast<py::int_>(val).cast<int32_t>());
    case LogicalTypeID::UINT16:
        return Value::createValue<uint16_t>(py::cast<py::int_>(val).cast<uint16_t>());
    case LogicalTypeID::INT16:
        return Value::createValue<int16_t>(py::cast<py::int_>(val).cast<int16_t>());
    case LogicalTypeID::UINT8:
        return Value::createValue<uint8_t>(py::cast<py::int_>(val).cast<uint8_t>());
    case LogicalTypeID::INT8:
        return Value::createValue<int8_t>(py::cast<py::int_>(val).cast<int8_t>());
    case LogicalTypeID::DOUBLE:
        return Value::createValue<double>(py::cast<py::float_>(val).cast<double>());
    case LogicalTypeID::STRING:
        if (py::isinstance<py::str>(val)) {
            return Value::createValue<std::string>(val.cast<std::string>());
        } else {
            return Value::createValue<std::string>(py::str(val));
        }
    case LogicalTypeID::TIMESTAMP: {
        // LCOV_EXCL_START
        if (!py::isinstance(val, datetime_datetime)) {
            throw RuntimeException("Error: parameter is not of type datetime.datetime, \
                but was resolved to type datetime.datetime");
        }
        // LCOV_EXCL_STOP
        auto ptr = val.ptr();
        auto year = PyDateTime_GET_YEAR(ptr);
        auto month = PyDateTime_GET_MONTH(ptr);
        auto day = PyDateTime_GET_DAY(ptr);
        auto hour = PyDateTime_DATE_GET_HOUR(ptr);
        auto minute = PyDateTime_DATE_GET_MINUTE(ptr);
        auto second = PyDateTime_DATE_GET_SECOND(ptr);
        auto micros = PyDateTime_DATE_GET_MICROSECOND(ptr);
        auto date = Date::fromDate(year, month, day);
        auto time = Time::fromTime(hour, minute, second, micros);
        return Value::createValue<timestamp_t>(Timestamp::fromDateTime(date, time));
    }
    case LogicalTypeID::DATE: {
        // LCOV_EXCL_START
        if (!py::isinstance(val, datetime_date)) {
            throw RuntimeException("Error: parameter is not of type datetime.date, \
                but was resolved to type datetime.date");
        }
        // LCOV_EXCL_STOP
        auto ptr = val.ptr();
        auto year = PyDateTime_GET_YEAR(ptr);
        auto month = PyDateTime_GET_MONTH(ptr);
        auto day = PyDateTime_GET_DAY(ptr);
        return Value::createValue<date_t>(Date::fromDate(year, month, day));
    }
    case LogicalTypeID::INTERVAL: {
        // LCOV_EXCL_START
        if (!py::isinstance(val, time_delta)) {
            throw RuntimeException("Error: parameter is not of type datetime.timedelta, \
                but was resolved to type datetime.timedelta");
        }
        // LCOV_EXCL_STOP
        auto ptr = val.ptr();
        auto days = PyDateTime_DELTA_GET_DAYS(ptr);
        auto seconds = PyDateTime_DELTA_GET_SECONDS(ptr);
        auto microseconds = PyDateTime_DELTA_GET_MICROSECONDS(ptr);
        interval_t interval;
        Interval::addition(interval, days, "days");
        Interval::addition(interval, seconds, "seconds");
        Interval::addition(interval, microseconds, "microseconds");
        return Value::createValue<interval_t>(interval);
    }
    case LogicalTypeID::UUID: {
        auto strVal = py::str(val).cast<std::string>();
        auto uuidVal = UUID::fromString(strVal);
        ku_uuid_t uuidToAppend;
        uuidToAppend.value = uuidVal;
        return Value{uuidToAppend};
    }
    case LogicalTypeID::LIST: {
        py::list lst = py::reinterpret_borrow<py::list>(val);
        std::vector<std::unique_ptr<Value>> children;
        for (auto child : lst) {
            children.push_back(std::make_unique<Value>(
                transformPythonValueAs(child, ListType::getChildType(type))));
        }
        return Value(std::make_unique<LogicalType>(type), std::move(children));
    }
    case LogicalTypeID::MAP: {
        py::dict dict = py::reinterpret_borrow<py::dict>(val);
        std::vector<std::unique_ptr<Value>> children;
        auto childKeyType = MapType::getKeyType(type), childValueType = MapType::getValueType(type);
        for (auto child : dict) {
            // type construction is inefficient, we have to create duplicates because it asks for
            // a unique ptr
            std::vector<StructField> fields;
            fields.emplace_back(InternalKeyword::MAP_KEY,
                std::make_unique<LogicalType>(childKeyType));
            fields.emplace_back(InternalKeyword::MAP_VALUE,
                std::make_unique<LogicalType>(childValueType));
            std::vector<std::unique_ptr<Value>> structValues;
            structValues.push_back(
                std::make_unique<Value>(transformPythonValueAs(child.first, childKeyType)));
            structValues.push_back(
                std::make_unique<Value>(transformPythonValueAs(child.second, childValueType)));
            children.push_back(std::make_unique<Value>(LogicalType::STRUCT(std::move(fields)),
                std::move(structValues)));
        }
        return Value(std::make_unique<LogicalType>(type), std::move(children));
    }
    // LCOV_EXCL_START
    default:
        KU_UNREACHABLE;
        // LCOV_EXCL_STOP
    }
}

Value PyConnection::transformPythonValue(const py::handle& val) {
    auto type = pyLogicalType(val);
    return transformPythonValueAs(val, *type);
}

std::unique_ptr<PyQueryResult> PyConnection::checkAndWrapQueryResult(
    std::unique_ptr<QueryResult>& queryResult) {
    if (!queryResult->isSuccess()) {
        throw std::runtime_error(queryResult->getErrorMessage());
    }
    auto pyQueryResult = std::make_unique<PyQueryResult>();
    pyQueryResult->queryResult = queryResult.release();
    pyQueryResult->isOwned = true;
    return pyQueryResult;
}

void PyConnection::createScalarFunction(const std::string& name, const py::function& udf,
    const py::list& params, const std::string& retval) {
    conn->addUDFFunctionSet(name, PyUDF::toFunctionSet(name, udf, params, retval));
}
