#include "actualclass.h"

ActualClass::ActualClass(double value){
    this->value_ = value;
}

double ActualClass::getValue()
{
  return this->value_;
}

double ActualClass::add(double toAdd)
{
  this->value_ += toAdd;
  return this->value_;
}