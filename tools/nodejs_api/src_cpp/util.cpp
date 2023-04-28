#include "include/util.h"

using namespace kuzu::common;

Napi::Object Util::GetObjectFromProperties(const vector<pair<std::string, unique_ptr<kuzu::common::Value>>>& properties, Napi::Env env) {
    Napi::Object nodeObj = Napi::Object::New(env);
    for (auto i = 0u; i < properties.size(); ++i) {
        auto& [name, value] = properties[i];
        nodeObj.Set(name, Util::ConvertToNapiObject(*value, env));
    }
    return nodeObj;
}

Napi::Value Util::ConvertToNapiObject(const kuzu::common::Value& value, Napi::Env env) {
    if (value.isNull()) {
        return Napi::Value();
    }
    auto dataType = value.getDataType();
    switch (dataType.typeID) {
    case kuzu::common::BOOL: {
        return Napi::Boolean::New(env, value.getValue<bool>());
    }
    case kuzu::common::INT64: {
        return Napi::Number::New(env, value.getValue<int64_t>());
    }
    case kuzu::common::DOUBLE: {
        return Napi::Number::New(env, value.getValue<double>());
    }
    case kuzu::common::STRING: {
        return Napi::String::New(env, value.getValue<string>());
    }
    case kuzu::common::DATE: {
        auto dateVal = value.getValue<date_t>();
        auto milliseconds  = ((int64_t)dateVal.days) * (kuzu::common::Interval::MICROS_PER_DAY) / kuzu::common::Interval::MICROS_PER_MSEC;
        return Napi::Date::New(env, milliseconds);
    }
    case kuzu::common::TIMESTAMP: {
        auto timestampVal = value.getValue<timestamp_t>();
        auto milliseconds = timestampVal.value / kuzu::common::Interval::MICROS_PER_MSEC;
        return Napi::Date::New(env, milliseconds);
    }
    case kuzu::common::INTERVAL: {
        auto intervalVal = value.getValue<interval_t>();
        auto days = Interval::DAYS_PER_MONTH * intervalVal.months + intervalVal.days;
        Napi::Object intervalObj = Napi::Object::New(env);
        intervalObj.Set("days", Napi::Number::New(env, days));
        intervalObj.Set("microseconds", Napi::Number::New(env, intervalVal.micros));
        return intervalObj;
    }
    case kuzu::common::VAR_LIST: {
        auto& listVal = value.getListValReference();
        Napi::Array arr = Napi::Array::New(env, listVal.size());
        for (auto i = 0u; i < listVal.size(); ++i) {
            arr.Set(i, ConvertToNapiObject(*listVal[i], env));
        }
        return arr;
    }
    case kuzu::common::NODE: {
        auto nodeVal = value.getValue<NodeVal>();
        Napi::Object nodeObj = GetObjectFromProperties(nodeVal.getProperties(), env);

        nodeObj.Set("_label", Napi::String::New(env, nodeVal.getLabelName()));

        Napi::Object nestedObj = Napi::Object::New(env);
        nestedObj.Set("offset", Napi::Number::New(env, nodeVal.getNodeID().offset));
        nestedObj.Set("table", Napi::Number::New(env, nodeVal.getNodeID().tableID));
        nodeObj.Set("_id", nestedObj);

        return nodeObj;
    }
    case kuzu::common::REL: {
        auto relVal = value.getValue<RelVal>();
        Napi::Object nodeObj = GetObjectFromProperties(relVal.getProperties(), env);

        Napi::Object nestedObjSrc = Napi::Object::New(env);
        nestedObjSrc.Set("offset", Napi::Number::New(env, relVal.getSrcNodeID().offset));
        nestedObjSrc.Set("table", Napi::Number::New(env, relVal.getSrcNodeID().tableID));
        nodeObj.Set("_src", nestedObjSrc);

        Napi::Object nestedObjDst = Napi::Object::New(env);
        nestedObjDst.Set("offset", Napi::Number::New(env, relVal.getDstNodeID().offset));
        nestedObjDst.Set("table", Napi::Number::New(env, relVal.getDstNodeID().tableID));
        nodeObj.Set("_dst", nestedObjDst);

        return nodeObj;
    }
    case kuzu::common::INTERNAL_ID: {
        auto internalIDVal = value.getValue<nodeID_t>();
        Napi::Object nestedObjDst = Napi::Object::New(env);
        nestedObjDst.Set("offset", Napi::Number::New(env, internalIDVal.offset));
        nestedObjDst.Set("table", Napi::Number::New(env, internalIDVal.tableID));
        return nestedObjDst;
    }
    default:
        Napi::TypeError::New(env, "Unsupported type: " + kuzu::common::Types::dataTypeToString(dataType));
    }
    return Napi::Value();
}

unordered_map<string, shared_ptr<kuzu::common::Value>> Util::transformParameters(Napi::Array params) {
    unordered_map<string, shared_ptr<kuzu::common::Value>> result;
    for (size_t i = 0; i < params.Length(); i++){
        Napi::Array param = params.Get(i).As<Napi::Array>();
        if (param.Length()!=2) {
            throw runtime_error("Each parameter must be in the form of <name, val>");
        } else if (!param.Get(uint32_t(0)).IsString()) {
            throw runtime_error("Parameter name must be of type string");
        }
        string name = param.Get(uint32_t(0)).ToString();
        auto transformedVal = transformNapiValue(param.Get(uint32_t(1)));
        result.insert({name, make_shared<kuzu::common::Value>(transformedVal)});
    }
    return result;
}

kuzu::common::Value Util::transformNapiValue(Napi::Value val) {
    if (val.IsBoolean()) {
        bool temp = val.ToBoolean();
        return Value::createValue<bool>(temp);
    } else if (val.IsNumber()) {
        double temp = val.ToNumber().DoubleValue();
        if (!isnan(temp) && (temp >= INT_MIN) && (temp <= INT_MAX) && ((double) ((int) temp) == temp)) {
            return Value::createValue<int64_t>(temp);
        } else {
            return Value::createValue<double_t>(temp);
        }
    } else if (val.IsString()) {
        string temp = val.ToString();
        return Value::createValue<string>(temp);
    } else if (val.IsDate()) {
        double dateMilliseconds = val.As<Napi::Date>().ValueOf();
        auto time = kuzu::common::Timestamp::FromEpochMs((int64_t) dateMilliseconds);
        if (kuzu::common::Timestamp::trunc(kuzu::common::DatePartSpecifier::DAY, time)
             == time) {
            return Value::createValue<date_t>(kuzu::common::Timestamp::GetDate(time));
        }
       return Value::createValue<timestamp_t>(time);
    } else {
        throw runtime_error("Unknown parameter type");
    }
}

