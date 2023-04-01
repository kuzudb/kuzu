#include "include/py_connection.h"

#include <iostream>

#include "binder/bound_statement_result.h"
#include "datetime.h" // from Python
#include "json.hpp"
#include "main/connection.h"
#include "planner/logical_plan/logical_plan.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;

void PyConnection::initialize(py::handle& m) {
    py::class_<PyConnection>(m, "Connection")
        .def(py::init<PyDatabase*, uint64_t>(), py::arg("database"), py::arg("num_threads") = 0)
        .def("execute", &PyConnection::execute, py::arg("prepared_statement"),
            py::arg("parameters") = py::list())
        .def("set_max_threads_for_exec", &PyConnection::setMaxNumThreadForExec,
            py::arg("num_threads"))
        .def("get_node_property_names", &PyConnection::getNodePropertyNames, py::arg("table_name"))
        .def("get_node_table_names", &PyConnection::getNodeTableNames)
        .def("get_rel_property_names", &PyConnection::getRelPropertyNames, py::arg("table_name"))
        .def("get_rel_table_names", &PyConnection::getRelTableNames)
        .def("prepare", &PyConnection::prepare, py::arg("query"))
        .def("set_query_timeout", &PyConnection::setQueryTimeout, py::arg("timeout_in_ms"))
        .def("get_num_nodes", &PyConnection::getNumNodes, py::arg("node_name"))
        .def("get_num_rels", &PyConnection::getNumRels, py::arg("rel_name"))
        .def("get_all_edges_for_torch_geometric", &PyConnection::getAllEdgesForTorchGeometric,
            py::arg("np_array"), py::arg("src_table_name"), py::arg("rel_name"),
            py::arg("dst_table_name"), py::arg("query_batch_size"))
        .def("scan_node_table_as_int64", &PyConnection::scanNodeProperty<std::int64_t>,
            py::arg("table_name"), py::arg("prop_name"), py::arg("indices"), py::arg("np_array"),
            py::arg("num_threads"))
        .def("scan_node_table_as_int32", &PyConnection::scanNodeProperty<std::int32_t>,
            py::arg("table_name"), py::arg("prop_name"), py::arg("indices"), py::arg("np_array"),
            py::arg("num_threads"))
        .def("scan_node_table_as_int16", &PyConnection::scanNodeProperty<std::int16_t>,
            py::arg("table_name"), py::arg("prop_name"), py::arg("indices"), py::arg("np_array"),
            py::arg("num_threads"))
        .def("scan_node_table_as_double", &PyConnection::scanNodeProperty<std::double_t>,
            py::arg("table_name"), py::arg("prop_name"), py::arg("indices"), py::arg("np_array"),
            py::arg("num_threads"))
        .def("scan_node_table_as_float", &PyConnection::scanNodeProperty<std::float_t>,
            py::arg("table_name"), py::arg("prop_name"), py::arg("indices"), py::arg("np_array"),
            py::arg("num_threads"))
        .def("scan_node_table_as_bool", &PyConnection::scanNodeProperty<bool>,
            py::arg("table_name"), py::arg("prop_name"), py::arg("indices"), py::arg("np_array"),
            py::arg("num_threads"));
    PyDateTime_IMPORT;
}

PyConnection::PyConnection(PyDatabase* pyDatabase, uint64_t numThreads) {
    storageDriver = std::make_unique<kuzu::main::StorageDriver>(pyDatabase->database.get());
    conn = std::make_unique<Connection>(pyDatabase->database.get());
    if (numThreads > 0) {
        conn->setMaxNumThreadForExec(numThreads);
    }
}

void PyConnection::setQueryTimeout(uint64_t timeoutInMS) {
    conn->setQueryTimeOut(timeoutInMS);
}

std::unique_ptr<PyQueryResult> PyConnection::execute(
    PyPreparedStatement* preparedStatement, py::list params) {
    auto parameters = transformPythonParameters(params);
    py::gil_scoped_release release;
    auto queryResult =
        conn->executeWithParams(preparedStatement->preparedStatement.get(), parameters);
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

py::str PyConnection::getNodePropertyNames(const std::string& tableName) {
    return conn->getNodePropertyNames(tableName);
}

py::str PyConnection::getNodeTableNames() {
    return conn->getNodeTableNames();
}

py::str PyConnection::getRelPropertyNames(const std::string& tableName) {
    return conn->getRelPropertyNames(tableName);
}

py::str PyConnection::getRelTableNames() {
    return conn->getRelTableNames();
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
    // Get the number of nodes in the src table for batching.
    auto numSrcNodes = getNumNodes(srcTableName);
    int64_t batches = numSrcNodes / queryBatchSize;
    if (numSrcNodes % queryBatchSize != 0) {
        batches += 1;
    }
    auto numRels = getNumRels(relName);

    auto buffer_info = npArray.request();
    auto buffer = (int64_t*)buffer_info.ptr;
    std::cout << "Done getting buffer for result numpy arrays." << std::endl;

    // Run queries in batch to fetch edges.
    auto queryString = "MATCH (a:{})-[:{}]->(b:{}) WHERE offset(id(a)) >= $s AND offset(id(a)) < "
                       "$e RETURN offset(id(a)), offset(id(b))";
    auto query = StringUtils::string_format(queryString, srcTableName, relName, dstTableName);
    auto preparedStatement = conn->prepare(query);
    for (int64_t batch = 0; batch < batches; ++batch) {
        int64_t start = batch * queryBatchSize;
        int64_t end = (batch + 1) * queryBatchSize;
        end = end > numSrcNodes ? numSrcNodes : end;
        std::unordered_map<std::string, std::shared_ptr<Value>> parameters;
        parameters["s"] = std::make_shared<Value>(start);
        parameters["e"] = std::make_shared<Value>(end);
        std::cout << "Batch " << batch << " from " << start << " to " << end << std::endl;
        auto result = conn->executeWithParams(preparedStatement.get(), parameters);
        if (!result->isSuccess()) {
            throw std::runtime_error(result->getErrorMessage());
        }
        auto srcBuffer = buffer;
        auto dstBuffer = buffer + numRels;

        auto table = result->getTable();
        auto tableSchema = table->getTableSchema();
        if (tableSchema->getColumn(0)->isFlat() && !tableSchema->getColumn(1)->isFlat()) {
            for (auto i = 0u; i < table->getNumTuples(); ++i) {
                auto tuple = table->getTuple(i);
                auto overflowValue =
                    (kuzu::common::overflow_value_t*)(tuple + tableSchema->getColOffset(1));
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
                auto overflowValue =
                    (kuzu::common::overflow_value_t*)(tuple + tableSchema->getColOffset(0));
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
    std::cout << "Done fetching edges." << std::endl;
}

template<class T>
void PyConnection::scanNodeProperty(const std::string& tableName, const std::string& propName,
    const py::array_t<uint64_t>& indices, py::array_t<T>& result, int numThreads) {
    auto indices_buffer_info = indices.request(false);
    auto indices_buffer = static_cast<uint64_t*>(indices_buffer_info.ptr);
    auto nodeOffsets = (offset_t*)indices_buffer;
    auto result_buffer_info = result.request();
    auto result_buffer = (uint8_t*)result_buffer_info.ptr;
    auto size = indices.size();
    storageDriver->scan(tableName, propName, nodeOffsets, size, result_buffer, numThreads);
}

std::unordered_map<std::string, std::shared_ptr<Value>> PyConnection::transformPythonParameters(
    py::list params) {
    std::unordered_map<std::string, std::shared_ptr<Value>> result;
    for (auto param : params) {
        if (!py::isinstance<py::tuple>(param)) {
            throw std::runtime_error("Each parameter must be in the form of <name, val>");
        }
        auto [name, val] = transformPythonParameter(param.cast<py::tuple>());
        result.insert({name, val});
    }
    return result;
}

std::pair<std::string, std::shared_ptr<Value>> PyConnection::transformPythonParameter(
    py::tuple param) {
    if (py::len(param) != 2) {
        throw std::runtime_error("Each parameter must be in the form of <name, val>");
    }
    if (!py::isinstance<py::str>(param[0])) {
        throw std::runtime_error("Parameter name must be of type string but get " +
                                 py::str(param[0].get_type()).cast<std::string>());
    }
    auto val = transformPythonValue(param[1]);
    return make_pair(param[0].cast<std::string>(), std::make_shared<Value>(val));
}

Value PyConnection::transformPythonValue(py::handle val) {
    auto datetime_mod = py::module::import("datetime");
    auto datetime_datetime = datetime_mod.attr("datetime");
    auto time_delta = datetime_mod.attr("timedelta");
    auto datetime_time = datetime_mod.attr("time");
    auto datetime_date = datetime_mod.attr("date");
    if (py::isinstance<py::bool_>(val)) {
        return Value::createValue<bool>(val.cast<bool>());
    } else if (py::isinstance<py::int_>(val)) {
        return Value::createValue<int64_t>(val.cast<int64_t>());
    } else if (py::isinstance<py::float_>(val)) {
        return Value::createValue<double_t>(val.cast<double_t>());
    } else if (py::isinstance<py::str>(val)) {
        return Value::createValue<std::string>(val.cast<std::string>());
    } else if (py::isinstance(val, datetime_datetime)) {
        auto ptr = val.ptr();
        auto year = PyDateTime_GET_YEAR(ptr);
        auto month = PyDateTime_GET_MONTH(ptr);
        auto day = PyDateTime_GET_DAY(ptr);
        auto hour = PyDateTime_DATE_GET_HOUR(ptr);
        auto minute = PyDateTime_DATE_GET_MINUTE(ptr);
        auto second = PyDateTime_DATE_GET_SECOND(ptr);
        auto micros = PyDateTime_DATE_GET_MICROSECOND(ptr);
        auto date = Date::FromDate(year, month, day);
        auto time = Time::FromTime(hour, minute, second, micros);
        return Value::createValue<timestamp_t>(Timestamp::FromDatetime(date, time));
    } else if (py::isinstance(val, datetime_date)) {
        auto ptr = val.ptr();
        auto year = PyDateTime_GET_YEAR(ptr);
        auto month = PyDateTime_GET_MONTH(ptr);
        auto day = PyDateTime_GET_DAY(ptr);
        return Value::createValue<date_t>(Date::FromDate(year, month, day));
    } else if (py::isinstance(val, time_delta)) {
        auto ptr = val.ptr();
        auto days = PyDateTime_DELTA_GET_DAYS(ptr);
        auto seconds = PyDateTime_DELTA_GET_SECONDS(ptr);
        auto microseconds = PyDateTime_DELTA_GET_MICROSECONDS(ptr);
        interval_t interval;
        Interval::addition(interval, days, "days");
        Interval::addition(interval, seconds, "seconds");
        Interval::addition(interval, microseconds, "microseconds");
        return Value::createValue<interval_t>(interval);
    } else {
        throw std::runtime_error(
            "Unknown parameter type " + py::str(val.get_type()).cast<std::string>());
    }
}
