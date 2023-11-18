#include "include/py_query_result_converter.h"

#include "common/types/value/value.h"
#include "include/py_query_result.h"

using namespace kuzu::common;

NPArrayWrapper::NPArrayWrapper(const LogicalType& type, uint64_t numFlatTuple)
    : type{type}, numElements{0} {
    data = py::array(convertToArrayType(type), numFlatTuple);
    dataBuffer = (uint8_t*)data.mutable_data();
    mask = py::array(py::dtype("bool"), numFlatTuple);
}

void NPArrayWrapper::appendElement(Value* value) {
    ((uint8_t*)mask.mutable_data())[numElements] = value->isNull();
    if (!value->isNull()) {
        switch (type.getLogicalTypeID()) {
        case LogicalTypeID::BOOL: {
            ((uint8_t*)dataBuffer)[numElements] = value->getValue<bool>();
        } break;
        case LogicalTypeID::INT128: {
            Int128_t::tryCast(value->getValue<int128_t>(), ((double_t*)dataBuffer)[numElements]);
        } break;
        case LogicalTypeID::INT64: {
            ((int64_t*)dataBuffer)[numElements] = value->getValue<int64_t>();
        } break;
        case LogicalTypeID::INT32: {
            ((int32_t*)dataBuffer)[numElements] = value->getValue<int32_t>();
        } break;
        case LogicalTypeID::INT16: {
            ((int16_t*)dataBuffer)[numElements] = value->getValue<int16_t>();
        } break;
        case LogicalTypeID::INT8: {
            ((int8_t*)dataBuffer)[numElements] = value->getValue<int8_t>();
        } break;
        case LogicalTypeID::UINT64: {
            ((uint64_t*)dataBuffer)[numElements] = value->getValue<uint64_t>();
        } break;
        case LogicalTypeID::UINT32: {
            ((uint32_t*)dataBuffer)[numElements] = value->getValue<uint32_t>();
        } break;
        case LogicalTypeID::UINT16: {
            ((uint16_t*)dataBuffer)[numElements] = value->getValue<uint16_t>();
        } break;
        case LogicalTypeID::UINT8: {
            ((uint8_t*)dataBuffer)[numElements] = value->getValue<uint8_t>();
        } break;
        case LogicalTypeID::DOUBLE: {
            ((double_t*)dataBuffer)[numElements] = value->getValue<double>();
        } break;
        case LogicalTypeID::FLOAT: {
            ((float_t*)dataBuffer)[numElements] = value->getValue<float>();
        } break;
        case LogicalTypeID::DATE: {
            ((int64_t*)dataBuffer)[numElements] =
                Date::getEpochNanoSeconds(value->getValue<date_t>());
        } break;
        case LogicalTypeID::TIMESTAMP: {
            ((int64_t*)dataBuffer)[numElements] =
                Timestamp::getEpochNanoSeconds(value->getValue<timestamp_t>());
        } break;
        case LogicalTypeID::INTERVAL: {
            ((int64_t*)dataBuffer)[numElements] =
                Interval::getNanoseconds(value->getValue<interval_t>());
        } break;
        case LogicalTypeID::STRING: {
            auto val = value->getValue<std::string>();
            auto result = PyUnicode_New(val.length(), 127);
            auto target_data = PyUnicode_DATA(result);
            memcpy(target_data, val.c_str(), val.length());
            ((PyObject**)dataBuffer)[numElements] = result;
        } break;
        case LogicalTypeID::BLOB: {
            ((py::bytes*)dataBuffer)[numElements] = PyQueryResult::convertValueToPyObject(*value);
        } break;
        case LogicalTypeID::UNION:
        case LogicalTypeID::MAP:
        case LogicalTypeID::STRUCT:
        case LogicalTypeID::NODE:
        case LogicalTypeID::REL: {
            ((py::object*)dataBuffer)[numElements] = PyQueryResult::convertValueToPyObject(*value);
        } break;
        case LogicalTypeID::FIXED_LIST:
        case LogicalTypeID::VAR_LIST: {
            ((py::list*)dataBuffer)[numElements] = PyQueryResult::convertValueToPyObject(*value);
        } break;
        case LogicalTypeID::RECURSIVE_REL: {
            ((py::dict*)dataBuffer)[numElements] = PyQueryResult::convertValueToPyObject(*value);
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
    }
    numElements++;
}

py::dtype NPArrayWrapper::convertToArrayType(const LogicalType& type) {
    std::string dtype;
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::INT128: {
        dtype = "float64";
    } break;
    case LogicalTypeID::INT64: {
        dtype = "int64";
    } break;
    case LogicalTypeID::INT32: {
        dtype = "int32";
    } break;
    case LogicalTypeID::INT16: {
        dtype = "int16";
    } break;
    case LogicalTypeID::INT8: {
        dtype = "int8";
    } break;
    case LogicalTypeID::UINT64: {
        dtype = "uint64";
    } break;
    case LogicalTypeID::UINT32: {
        dtype = "uint32";
    } break;
    case LogicalTypeID::UINT16: {
        dtype = "uint16";
    } break;
    case LogicalTypeID::UINT8: {
        dtype = "uint8";
    } break;
    case LogicalTypeID::DOUBLE: {
        dtype = "float64";
    } break;
    case LogicalTypeID::FLOAT: {
        dtype = "float32";
    } break;
    case LogicalTypeID::BOOL: {
        dtype = "bool";
        break;
    }
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP: {
        dtype = "datetime64[ns]";
        break;
    }
    case LogicalTypeID::INTERVAL: {
        dtype = "timedelta64[ns]";
        break;
    }
    case LogicalTypeID::UNION:
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST:
    case LogicalTypeID::STRING:
    case LogicalTypeID::MAP:
    case LogicalTypeID::RECURSIVE_REL: {
        dtype = "object";
        break;
    }
    default: {
        KU_UNREACHABLE;
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
