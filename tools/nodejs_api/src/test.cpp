#include <iostream>

#include "main/kuzu.h"
#include <napi.h>
using namespace kuzu::main;

using namespace Napi;

Napi::Object Method(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (info.Length() != 1) {
        Napi::TypeError::New(env, "Wrong number of arguments").ThrowAsJavaScriptException();
        return Object::New(env);
    }
    if (!info[0].IsString()) {
        Napi::TypeError::New(env, "Wrong arguments").ThrowAsJavaScriptException();
        return Object::New(env);
    }

    std::cout << "Arg: " << info[0].ToString().Utf8Value().c_str() << std::endl;

    DatabaseConfig databaseConfig("test");
    SystemConfig systemConfig(1ull << 30 /* set buffer manager size to 2GB */);
    Database database(databaseConfig, systemConfig);

    auto connection = Connection(&database);
    connection.query("create node table person (ID INt64, fName StRING, gender INT64, isStudent "
                     "BoOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, "
                     "registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], "
                     "usedNames STRING[], courseScoresPerTerm INT64[][], PRIMARY KEY (ID));");
    connection.query("COPY person FROM \"../../dataset/tinysnb/vPerson.csv\" (HEADER=true);");

    auto result =
        connection.query("MATCH (a:person) RETURN a.fName, a.age, a.eyeSight, a.isStudent;");
    Napi::Object output = Object::New(env);
    auto i = 0;
    while (result->hasNext()) {
        auto row = result->getNext();
        Napi::Object obj = Object::New(env);
        std::string fName = row->getResultValue(0)->getValue<std::string>();
        obj.Set("fName", Napi::String::New(env, fName));
        obj.Set("gender", Napi::Number::New(env, row->getResultValue(1)->getValue<int64_t>()));
        obj.Set("eyeSight", Napi::Number::New(env, row->getResultValue(2)->getValue<double>()));
        obj.Set("isStudent", row->getResultValue(3)->getValue<bool>());
        output.Set(uint32_t(i), obj);
        ++i;
    }

    return output;
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
    exports.Set(Napi::String::New(env, "test"), Napi::Function::New(env, Method));
    return exports;
}

NODE_API_MODULE(addon, Init)
