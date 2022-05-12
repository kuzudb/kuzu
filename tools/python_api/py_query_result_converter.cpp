#include "include/py_query_result_converter.h"

#include "include/py_query_result.h"
using namespace graphflow::common;

NPArrayWrapper::NPArrayWrapper(const DataType& type, uint64_t numFlatTuple)
    : type{type}, numElements{0} {
    data = py::array(convertToArrayType(type), numFlatTuple);
    dataBuffer = (uint8_t*)data.mutable_data();
    mask = py::array(py::dtype("bool"), numFlatTuple);
}

void NPArrayWrapper::appendElement(Value* value, bool isNull) {
    ((uint8_t*)mask.mutable_data())[numElements] = isNull;
    if (!isNull) {
        switch (type.typeID) {
        case BOOL: {
            ((uint8_t*)dataBuffer)[numElements] = value->val.booleanVal;
            break;
        }
        case INT64: {
            ((int64_t*)dataBuffer)[numElements] = value->val.int64Val;
            break;
        }
        case DOUBLE: {
            ((double_t*)dataBuffer)[numElements] = value->val.doubleVal;
            break;
        }
        case DATE: {
            ((int64_t*)dataBuffer)[numElements] = Date::getEpochNanoSeconds(value->val.dateVal);
            break;
        }
        case TIMESTAMP: {
            ((int64_t*)dataBuffer)[numElements] =
                Timestamp::getEpochNanoSeconds(value->val.timestampVal);
            break;
        }
        case INTERVAL: {
            ((int64_t*)dataBuffer)[numElements] = Interval::getNanoseconds(value->val.intervalVal);
            break;
        }
        case STRING: {
            auto val = value->val.strVal;
            auto result = PyUnicode_New(val.len, 127);
            auto target_data = PyUnicode_DATA(result);
            memcpy(target_data, val.getData(), val.len);
            ((PyObject**)dataBuffer)[numElements] = result;
            break;
        }
        case LIST: {
            ((py::list*)dataBuffer)[numElements] =
                PyQueryResult::convertValueToPyObject(*value, false /* isNull */);
            break;
        }
        case UNSTRUCTURED: {
            auto str = TypeUtils::toString(*value);
            ((PyObject**)dataBuffer)[numElements] =
                PyUnicode_FromStringAndSize(str.c_str(), str.size());
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
    case UNSTRUCTURED:
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
            columns[i]->appendElement(flatTuple->getValue(i), flatTuple->nullMask[i]);
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
