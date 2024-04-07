#include "include/py_connection.h"

#include <utility>

#include "common/constants.h"
#include "cached_import/py_cached_import.h"
#include "common/string_format.h"
#include "common/types/uuid.h"
#include "datetime.h" // from Python
#include "function/built_in_function_utils.h"
#include "main/connection.h"
#include "pandas/pandas_scan.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu;

void PyConnection::initialize(py::handle& m) {
    py::class_<PyConnection>(m, "Connection")
        .def(py::init<PyDatabase*, uint64_t>(), py::arg("database"), py::arg("num_threads") = 0)
        .def("execute", &PyConnection::execute, py::arg("prepared_statement"),
            py::arg("parameters") = py::dict())
        .def("set_max_threads_for_exec", &PyConnection::setMaxNumThreadForExec,
            py::arg("num_threads"))
        .def("prepare", &PyConnection::prepare, py::arg("query"))
        .def("set_query_timeout", &PyConnection::setQueryTimeout, py::arg("timeout_in_ms"))
        .def("get_num_nodes", &PyConnection::getNumNodes, py::arg("node_name"))
        .def("get_num_rels", &PyConnection::getNumRels, py::arg("rel_name"))
        .def("get_all_edges_for_torch_geometric", &PyConnection::getAllEdgesForTorchGeometric,
            py::arg("np_array"), py::arg("src_table_name"), py::arg("rel_name"),
            py::arg("dst_table_name"), py::arg("query_batch_size"));
    PyDateTime_IMPORT;
}

PyConnection::PyConnection(PyDatabase* pyDatabase, uint64_t numThreads) {
    storageDriver = std::make_unique<kuzu::main::StorageDriver>(pyDatabase->database.get());
    conn = std::make_unique<Connection>(pyDatabase->database.get());
    conn->getClientContext()->addScanReplace(function::ScanReplacement(kuzu::replacePD));
    if (numThreads > 0) {
        conn->setMaxNumThreadForExec(numThreads);
    }
}

void PyConnection::setQueryTimeout(uint64_t timeoutInMS) {
    conn->setQueryTimeOut(timeoutInMS);
}

static std::unordered_map<std::string, std::unique_ptr<Value>> transformPythonParameters(
    const py::dict& params, Connection* conn);

std::unique_ptr<PyQueryResult> PyConnection::execute(
    PyPreparedStatement* preparedStatement, const py::dict& params) {
    auto parameters = transformPythonParameters(params, conn.get());
    py::gil_scoped_release release;
    auto queryResult =
        conn->executeWithParams(preparedStatement->preparedStatement.get(), std::move(parameters));
    py::gil_scoped_acquire acquire;
    if (!queryResult->isSuccess()) {
        throw std::runtime_error(queryResult->getErrorMessage());
    }
    auto pyQueryResult = std::make_unique<PyQueryResult>();
    pyQueryResult->queryResult = std::move(queryResult);
    return pyQueryResult;
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

static Value transformPythonValue(py::handle val);

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
        auto val = std::make_unique<Value>(transformPythonValue(value));
        result.insert({name, std::move(val)});
    }
    return result;
}

static bool canCastPyLogicalType(const LogicalType& from, const LogicalType& to) {
    // the input of this function is restricted to the output of pyLogicalType
    if (from.getLogicalTypeID() == LogicalTypeID::ANY ||
        from.getLogicalTypeID() == to.getLogicalTypeID()) {
        return true;
    } else if (to.getLogicalTypeID() == LogicalTypeID::ANY) {
        return false;
    } else if (from.getLogicalTypeID() == LogicalTypeID::MAP) {
        if (to.getLogicalTypeID() != LogicalTypeID::MAP) {
            return false;
        }
        auto fromKeyType = MapType::getKeyType(&from), fromValueType = MapType::getValueType(&to);
        auto toKeyType = MapType::getKeyType(&to), toValueType = MapType::getValueType(&to);
        return
            (canCastPyLogicalType(*fromKeyType, *toKeyType) ||
            canCastPyLogicalType(*toKeyType, *fromKeyType)) &&
            (canCastPyLogicalType(*fromValueType, *toValueType) ||
            canCastPyLogicalType(*toValueType, *fromValueType));
    } else if (from.getLogicalTypeID() == LogicalTypeID::LIST) {
        if (to.getLogicalTypeID() != LogicalTypeID::LIST) {
            return false;
        }
        return canCastPyLogicalType(
            *ListType::getChildType(&from), *ListType::getChildType(&to));
    } else {
        auto castCost = function::BuiltInFunctionsUtils::getCastCost(
            from.getLogicalTypeID(), to.getLogicalTypeID());
        return castCost != UNDEFINED_CAST_COST;
    }
    return false;
}

static void tryConvertPyLogicalType(LogicalType& from, LogicalType& to);

static std::unique_ptr<LogicalType> castPyLogicalType(const LogicalType& from, const LogicalType& to) {
    // assumes from can cast to to
    if (from.getLogicalTypeID() == LogicalTypeID::MAP) {
        auto fromKeyType = MapType::getKeyType(&from), fromValueType = MapType::getValueType(&from);
        auto toKeyType = MapType::getKeyType(&to), toValueType = MapType::getValueType(&to);
        auto outputKeyType = canCastPyLogicalType(*fromKeyType, *toKeyType) ?
            castPyLogicalType(*fromKeyType, *toKeyType) : castPyLogicalType(*toKeyType, *fromKeyType);
        auto outputValueType = canCastPyLogicalType(*fromValueType, *toValueType) ?
            castPyLogicalType(*fromValueType, *toValueType) : castPyLogicalType(*toValueType, *fromValueType);
        return LogicalType::MAP(std::move(outputKeyType), std::move(outputValueType));
    }
    return std::make_unique<LogicalType>(to);
}

void tryConvertPyLogicalType(LogicalType& from, LogicalType& to) {
    if (canCastPyLogicalType(from, to)) {
        from = *castPyLogicalType(from, to);
    } else if (canCastPyLogicalType(to, from)) {
        to = *castPyLogicalType(to, from);
    } else {
        throw RuntimeException(stringFormat(
            "Cannot convert Python object to Kuzu value : {}  is incompatible with {}",
            from.toString(), to.toString()));
    }
}

static std::unique_ptr<LogicalType> pyLogicalType(py::handle val) {
    auto datetime_datetime = importCache->datetime.datetime();
    auto time_delta = importCache->datetime.timedelta();
    auto datetime_date = importCache->datetime.date();
    auto uuid = importCache->uuid.UUID();
    if (val.is_none()) {
        return LogicalType::ANY();
    } else if (py::isinstance<py::bool_>(val)) {
        return LogicalType::BOOL();
    } else if (py::isinstance<py::int_>(val)) {
        return LogicalType::INT64();
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
            tryConvertPyLogicalType(*childType, *curChildType);
        }
        return LogicalType::LIST(std::move(childType));
    } else if (py::isinstance<py::dict>(val)) {
        py::dict dict = py::reinterpret_borrow<py::dict>(val);
        auto childKeyType = LogicalType::ANY(), childValueType = LogicalType::ANY();
        for (auto child : dict) {
            auto curChildKeyType = pyLogicalType(child.first),
                curChildValueType = pyLogicalType(child.second);
            tryConvertPyLogicalType(*childKeyType, *curChildKeyType);
            tryConvertPyLogicalType(*childValueType, *curChildValueType);
        }
        return LogicalType::MAP(std::move(childKeyType), std::move(childValueType));
    } else {
        // LCOV_EXCL_START
        throw common::RuntimeException(
            "Unknown parameter type " + py::str(val.get_type()).cast<std::string>());
        // LCOV_EXCL_STOP
    }
}

static Value transformPythonValueAs(py::handle val, const LogicalType* type) {
    // ignore the type of the actual python object, just directly cast
    if (val.is_none()) {
        return Value::createNullValue(*type);
    }
    switch (type->getLogicalTypeID()) {
    case LogicalTypeID::ANY:
        return Value::createNullValue();
    case LogicalTypeID::BOOL:
        return Value::createValue<bool>(val.cast<bool>());
    case LogicalTypeID::INT64:
        return Value::createValue<int64_t>(val.cast<int64_t>());
    case LogicalTypeID::DOUBLE:
        return Value::createValue<double>(val.cast<double>());
    case LogicalTypeID::STRING:
        if (py::isinstance<py::str>(val)) {
            return Value::createValue<std::string>(val.cast<std::string>());
        } else {
            return Value::createValue<std::string>(py::str(val));
        }
    case LogicalTypeID::TIMESTAMP: {
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
        auto ptr = val.ptr();
        auto year = PyDateTime_GET_YEAR(ptr);
        auto month = PyDateTime_GET_MONTH(ptr);
        auto day = PyDateTime_GET_DAY(ptr);
        return Value::createValue<date_t>(Date::fromDate(year, month, day));
    }
    case LogicalTypeID::INTERVAL: {
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
        return Value(std::make_unique<LogicalType>(*type), std::move(children));
    }
    case LogicalTypeID::MAP: {
        py::dict dict = py::reinterpret_borrow<py::dict>(val);
        std::vector<std::unique_ptr<Value>> children;
        auto childKeyType = MapType::getKeyType(type),
            childValueType = MapType::getValueType(type);
        for (auto child : dict) {
            // type construction is inefficient, we have to create duplicates because it asks for
            // a unique ptr
            std::vector<StructField> fields;
            fields.emplace_back(
                InternalKeyword::MAP_KEY, std::make_unique<LogicalType>(*childKeyType));
            fields.emplace_back(
                InternalKeyword::MAP_VALUE, std::make_unique<LogicalType>(*childValueType));
            std::vector<std::unique_ptr<Value>> structValues;
            structValues.push_back(std::make_unique<Value>(transformPythonValueAs(child.first, childKeyType)));
            structValues.push_back(std::make_unique<Value>(transformPythonValueAs(child.second, childValueType)));
            children.push_back(std::make_unique<Value>(
                LogicalType::STRUCT(std::move(fields)),
                std::move(structValues)));
        }
        return Value(std::make_unique<LogicalType>(*type), std::move(children));
    }
    // LCOV_EXCL_START
    default:
        KU_UNREACHABLE;
    // LCOV_EXCL_STOP
    }
}

Value transformPythonValue(py::handle val) {
    auto type = pyLogicalType(val);
    return transformPythonValueAs(val, type.get());
}
