#include "include/py_query_result_converter.h"

#include "common/types/value.h"
#include "include/py_query_result.h"
#include "processor/result/flat_tuple.h"

using namespace kuzu::common;

NPArrayWrapper::NPArrayWrapper(const DataType& type, uint64_t numFlatTuple)
    : type{type}, numElements{0} {
    data = py::array(convertToArrayType(type), numFlatTuple);
    dataBuffer = (uint8_t*)data.mutable_data();
    mask = py::array(py::dtype("bool"), numFlatTuple);
}

void NPArrayWrapper::appendElement(Value* value) {
    ((uint8_t*)mask.mutable_data())[numElements] = value->isNull();
    if (!value->isNull()) {
        switch (type.typeID) {
        case DataTypeID::BOOL: {
            ((uint8_t*)dataBuffer)[numElements] = value->getValue<bool>();
            break;
        }
        case DataTypeID::INT64: {
            ((int64_t*)dataBuffer)[numElements] = value->getValue<int64_t>();
            break;
        }
        case DataTypeID::DOUBLE: {
            ((double_t*)dataBuffer)[numElements] = value->getValue<double>();
            break;
        }
        case DataTypeID::DATE: {
            ((int64_t*)dataBuffer)[numElements] =
                Date::getEpochNanoSeconds(value->getValue<date_t>());
            break;
        }
        case DataTypeID::TIMESTAMP: {
            ((int64_t*)dataBuffer)[numElements] =
                Timestamp::getEpochNanoSeconds(value->getValue<timestamp_t>());
            break;
        }
        case DataTypeID::INTERVAL: {
            ((int64_t*)dataBuffer)[numElements] =
                Interval::getNanoseconds(value->getValue<interval_t>());
            break;
        }
        case DataTypeID::STRING: {
            auto val = value->getValue<std::string>();
            auto result = PyUnicode_New(val.length(), 127);
            auto target_data = PyUnicode_DATA(result);
            memcpy(target_data, val.c_str(), val.length());
            ((PyObject**)dataBuffer)[numElements] = result;
            break;
        }
        case DataTypeID::NODE:
        case DataTypeID::REL: {
            ((py::dict*)dataBuffer)[numElements] = PyQueryResult::convertValueToPyObject(*value);
            break;
        }
        case DataTypeID::VAR_LIST: {
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
    std::string dtype;
    switch (type.typeID) {
    case DataTypeID::INT64: {
        dtype = "int64";
        break;
    }
    case DataTypeID::DOUBLE: {
        dtype = "float64";
        break;
    }
    case DataTypeID::BOOL: {
        dtype = "bool";
        break;
    }
    case DataTypeID::NODE:
    case DataTypeID::REL:
    case DataTypeID::VAR_LIST:
    case DataTypeID::STRING: {
        dtype = "object";
        break;
    }
    case DataTypeID::DATE:
    case DataTypeID::TIMESTAMP: {
        dtype = "datetime64[ns]";
        break;
    }
    case DataTypeID::INTERVAL: {
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
        columns.emplace_back(std::make_unique<NPArrayWrapper>(type, queryResult->getNumTuples()));
    }
}

py::object QueryResultConverter::toDF() {
    queryResult->resetIterator();
    while (queryResult->hasNext()) {
        auto flatTuple = queryResult->getNext();
        for (auto i = 0u; i < columns.size(); i++) {
            columns[i]->appendElement(flatTuple->getValue(i));
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
