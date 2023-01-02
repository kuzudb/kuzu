#include "include/py_query_result_converter.h"

#include "include/py_query_result.h"
using namespace kuzu::common;

NPArrayWrapper::NPArrayWrapper(const DataType& type, uint64_t numFlatTuple)
    : type{type}, numElements{0} {
    data = py::array(convertToArrayType(type), numFlatTuple);
    dataBuffer = (uint8_t*)data.mutable_data();
    mask = py::array(py::dtype("bool"), numFlatTuple);
}

void NPArrayWrapper::appendElement(ResultValue* value) {
    ((uint8_t*)mask.mutable_data())[numElements] = value->isNullVal();
    if (!value->isNullVal()) {
        switch (type.typeID) {
        case BOOL: {
            ((uint8_t*)dataBuffer)[numElements] = value->getBooleanVal();
            break;
        }
        case INT64: {
            ((int64_t*)dataBuffer)[numElements] = value->getInt64Val();
            break;
        }
        case DOUBLE: {
            ((double_t*)dataBuffer)[numElements] = value->getDoubleVal();
            break;
        }
        case DATE: {
            ((int64_t*)dataBuffer)[numElements] = Date::getEpochNanoSeconds(value->getDateVal());
            break;
        }
        case TIMESTAMP: {
            ((int64_t*)dataBuffer)[numElements] =
                Timestamp::getEpochNanoSeconds(value->getTimestampVal());
            break;
        }
        case INTERVAL: {
            ((int64_t*)dataBuffer)[numElements] = Interval::getNanoseconds(value->getIntervalVal());
            break;
        }
        case STRING: {
            auto val = value->getStringVal();
            auto result = PyUnicode_New(val.length(), 127);
            auto target_data = PyUnicode_DATA(result);
            memcpy(target_data, val.c_str(), val.length());
            ((PyObject**)dataBuffer)[numElements] = result;
            break;
        }
        case LIST: {
            ((py::list*)dataBuffer)[numElements] = PyQueryResult::convertValueToPyObject(*value);
            break;
        }
        default: {
            assert(false);
        }
        }
    }
    numElements++;
}

py::dtype NPArrayWrapper::convertToArrayType(const DataType& type) {
    string dtype;
    switch (type.typeID) {
    case INT64: {
        dtype = "int64";
        break;
    }
    case DOUBLE: {
        dtype = "float64";
        break;
    }
    case BOOL: {
        dtype = "bool";
        break;
    }
    case LIST:
    case STRING: {
        dtype = "object";
        break;
    }
    case DATE:
    case TIMESTAMP: {
        dtype = "datetime64[ns]";
        break;
    }
    case INTERVAL: {
        dtype = "timedelta64[ns]";
        break;
    }
    default: {
        assert(false);
    }
    }
    return py::dtype(dtype);
}

QueryResultConverter::QueryResultConverter(QueryResult* queryResult) : queryResult{queryResult} {
    for (auto& type : queryResult->getColumnDataTypes()) {
        columns.emplace_back(make_unique<NPArrayWrapper>(type, queryResult->getNumTuples()));
    }
}

py::object QueryResultConverter::toDF() {
    while (queryResult->hasNext()) {
        auto flatTuple = queryResult->getNext();
        for (auto i = 0u; i < columns.size(); i++) {
            columns[i]->appendElement(flatTuple->getResultValue(i));
        }
    }
    py::dict result;
    auto colNames = queryResult->getColumnNames();

    for (auto i = 0u; i < colNames.size(); i++) {
        result[colNames[i].c_str()] =
            py::module::import("numpy.ma").attr("masked_array")(columns[i]->data, columns[i]->mask);
    }
    return py::module::import("pandas").attr("DataFrame").attr("from_dict")(result);
}
