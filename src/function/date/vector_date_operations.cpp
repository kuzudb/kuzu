#include "include/vector_date_operations.h"

#include "operations/include/date_operations.h"

namespace graphflow {
namespace function {

vector<unique_ptr<VectorOperationDefinition>> DayNameVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(DAYNAME_FUNC_NAME, vector<DataTypeID>{DATE}, STRING,
            UnaryExecFunction<date_t, gf_string_t, operation::DayName>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> MonthNameVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(MONTHNAME_FUNC_NAME, vector<DataTypeID>{DATE},
            STRING, UnaryExecFunction<date_t, gf_string_t, operation::MonthName>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> LastDayVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(LASTDAY_FUNC_NAME,
        vector<DataTypeID>{DATE}, DATE, UnaryExecFunction<date_t, date_t, operation::LastDay>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> DatePartVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(DATEPART_FUNC_NAME, vector<DataTypeID>{DATE, STRING},
            INT64, BinaryExecFunction<date_t, gf_string_t, int64_t, operation::DatePart>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> DateTruncVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(DATETRUNC_FUNC_NAME,
        vector<DataTypeID>{DATE, STRING}, DATE,
        BinaryExecFunction<date_t, gf_string_t, date_t, operation::DateTrunc>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> GreatestVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(GREATEST_FUNC_NAME, vector<DataTypeID>{DATE, DATE},
            DATE, BinaryExecFunction<date_t, date_t, date_t, operation::Greatest>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> LeastVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(LEAST_FUNC_NAME, vector<DataTypeID>{DATE, DATE},
            DATE, BinaryExecFunction<date_t, date_t, date_t, operation::Least>));
    return result;
}

} // namespace function
} // namespace graphflow
