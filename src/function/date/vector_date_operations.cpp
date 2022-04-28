#include "include/vector_date_operations.h"

#include "operations/include/date_operations.h"

namespace graphflow {
namespace function {

vector<unique_ptr<VectorOperationDefinition>> DayNameVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(DAYNAME_FUNC_NAME, vector<DataTypeID>{DATE}, STRING,
            UnaryExecFunction<date_t, gf_string_t, operation::DayName>));
    result.push_back(
        make_unique<VectorOperationDefinition>(DAYNAME_FUNC_NAME, vector<DataTypeID>{TIMESTAMP},
            STRING, UnaryExecFunction<timestamp_t, gf_string_t, operation::DayName>));
    result.push_back(
        make_unique<VectorOperationDefinition>(DAYNAME_FUNC_NAME, vector<DataTypeID>{UNSTRUCTURED},
            STRING, UnaryExecFunction<Value, gf_string_t, operation::DayName>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> MonthNameVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(MONTHNAME_FUNC_NAME, vector<DataTypeID>{DATE},
            STRING, UnaryExecFunction<date_t, gf_string_t, operation::MonthName>));
    result.push_back(
        make_unique<VectorOperationDefinition>(MONTHNAME_FUNC_NAME, vector<DataTypeID>{TIMESTAMP},
            STRING, UnaryExecFunction<timestamp_t, gf_string_t, operation::MonthName>));
    result.push_back(make_unique<VectorOperationDefinition>(MONTHNAME_FUNC_NAME,
        vector<DataTypeID>{UNSTRUCTURED}, STRING,
        UnaryExecFunction<Value, gf_string_t, operation::MonthName>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> LastDayVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(LAST_DAY_FUNC_NAME,
        vector<DataTypeID>{DATE}, DATE, UnaryExecFunction<date_t, date_t, operation::LastDay>));
    result.push_back(
        make_unique<VectorOperationDefinition>(LAST_DAY_FUNC_NAME, vector<DataTypeID>{TIMESTAMP},
            DATE, UnaryExecFunction<timestamp_t, date_t, operation::LastDay>));
    result.push_back(
        make_unique<VectorOperationDefinition>(LAST_DAY_FUNC_NAME, vector<DataTypeID>{UNSTRUCTURED},
            DATE, UnaryExecFunction<Value, date_t, operation::LastDay>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> DatePartVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_FUNC_NAME,
        vector<DataTypeID>{STRING, DATE}, INT64,
        BinaryExecFunction<gf_string_t, date_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_FUNC_NAME,
        vector<DataTypeID>{STRING, TIMESTAMP}, INT64,
        BinaryExecFunction<gf_string_t, timestamp_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_FUNC_NAME,
        vector<DataTypeID>{STRING, INTERVAL}, INT64,
        BinaryExecFunction<gf_string_t, interval_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_FUNC_NAME,
        vector<DataTypeID>{STRING, UNSTRUCTURED}, INT64,
        BinaryExecFunction<gf_string_t, Value, int64_t, operation::DatePart>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> DateTruncVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(DATE_TRUNC_FUNC_NAME,
        vector<DataTypeID>{STRING, DATE}, DATE,
        BinaryExecFunction<gf_string_t, date_t, date_t, operation::DateTrunc>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_TRUNC_FUNC_NAME,
        vector<DataTypeID>{STRING, TIMESTAMP}, TIMESTAMP,
        BinaryExecFunction<gf_string_t, timestamp_t, timestamp_t, operation::DateTrunc>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_TRUNC_FUNC_NAME,
        vector<DataTypeID>{STRING, UNSTRUCTURED}, UNSTRUCTURED,
        BinaryExecFunction<gf_string_t, Value, Value, operation::DateTrunc>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> GreatestVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(GREATEST_FUNC_NAME, vector<DataTypeID>{DATE, DATE},
            DATE, BinaryExecFunction<date_t, date_t, date_t, operation::Greatest>));
    result.push_back(make_unique<VectorOperationDefinition>(GREATEST_FUNC_NAME,
        vector<DataTypeID>{TIMESTAMP, TIMESTAMP}, TIMESTAMP,
        BinaryExecFunction<timestamp_t, timestamp_t, timestamp_t, operation::Greatest>));
    result.push_back(make_unique<VectorOperationDefinition>(GREATEST_FUNC_NAME,
        vector<DataTypeID>{UNSTRUCTURED, UNSTRUCTURED}, UNSTRUCTURED,
        BinaryExecFunction<Value, Value, Value, operation::Greatest>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> LeastVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(LEAST_FUNC_NAME, vector<DataTypeID>{DATE, DATE},
            DATE, BinaryExecFunction<date_t, date_t, date_t, operation::Least>));
    result.push_back(make_unique<VectorOperationDefinition>(LEAST_FUNC_NAME,
        vector<DataTypeID>{TIMESTAMP, TIMESTAMP}, TIMESTAMP,
        BinaryExecFunction<timestamp_t, timestamp_t, timestamp_t, operation::Least>));
    result.push_back(make_unique<VectorOperationDefinition>(LEAST_FUNC_NAME,
        vector<DataTypeID>{UNSTRUCTURED, UNSTRUCTURED}, UNSTRUCTURED,
        BinaryExecFunction<Value, Value, Value, operation::Least>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> MakeDateVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(MAKE_DATE_FUNC_NAME,
        vector<DataTypeID>{INT64, INT64, INT64}, DATE,
        TernaryExecFunction<int64_t, int64_t, int64_t, date_t, operation::MakeDate>));
    return result;
}

} // namespace function
} // namespace graphflow
