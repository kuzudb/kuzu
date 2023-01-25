#include "classexample.h"
#include "main/kuzu.h"

using namespace kuzu::main;

Napi::FunctionReference ClassExample::constructor;

Napi::Object ClassExample::Init(Napi::Env env, Napi::Object exports) {
  Napi::HandleScope scope(env);

  Napi::Function t = DefineClass(env, "ClassExample", {
      InstanceMethod("temp", &ClassExample::Temp),
      InstanceMethod("add", &ClassExample::Add),
      InstanceMethod("getValue", &ClassExample::GetValue),
  });

  constructor = Napi::Persistent(t);
  constructor.SuppressDestruct();

  exports.Set("ClassExample", t);
  return exports;
}


ClassExample::ClassExample(const Napi::CallbackInfo& info) : Napi::ObjectWrap<ClassExample>(info)  {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  int length = info.Length();
  
  if (length != 1) {
    Napi::TypeError::New(env, "Only one argument expected").ThrowAsJavaScriptException();
  }

  if(!info[0].IsNumber()){
    Napi::Object object_parent = info[0].As<Napi::Object>();
    ClassExample* example_parent = Napi::ObjectWrap<ClassExample>::Unwrap(object_parent);
    ActualClass* parent_actual_class_instance = example_parent->GetInternalInstance();
    this->actualClass_ = new ActualClass(parent_actual_class_instance->getValue());
    return;
  }

  Napi::Number value = info[0].As<Napi::Number>();
  this->actualClass_ = new ActualClass(value.DoubleValue());
}

Napi::Value ClassExample::Temp(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (info.Length() != 1) {
      Napi::TypeError::New(env, "Wrong number of arguments").ThrowAsJavaScriptException();
      return Napi::Object::New(env);
  }
  if (!info[0].IsString()) {
      Napi::TypeError::New(env, "Wrong arguments").ThrowAsJavaScriptException();
      return Napi::Object::New(env);
  }

  std::cout << "Arg: " << info[0].ToString().Utf8Value().c_str() << std::endl;

  std::string fields = info[0].ToString().Utf8Value().c_str();

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
      connection.query("MATCH (a:person { fName: \"Alice\" }) RETURN a.fName, a.age, a.eyeSight, a.isStudent;");

  Napi::Object output = Napi::Object::New(env);
  auto i = 0;
  while (result->hasNext()) {
    auto row = result->getNext();
    Napi::Object obj = Napi::Object::New(env);
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

Napi::Value ClassExample::GetValue(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  double num = this->actualClass_->getValue();
  return Napi::Number::New(env, num);
}

Napi::Value ClassExample::Add(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (info.Length() != 1 || !info[0].IsNumber()) {
    Napi::TypeError::New(env, "Number expected").ThrowAsJavaScriptException();
  }

  Napi::Number toAdd = info[0].As<Napi::Number>();
  double answer = this->actualClass_->add(toAdd.DoubleValue());

  return Napi::Number::New(info.Env(), answer);
}

ActualClass* ClassExample::GetInternalInstance() {
  return this->actualClass_;
}