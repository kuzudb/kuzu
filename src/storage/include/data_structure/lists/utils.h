#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <memory>

using namespace std;

namespace graphflow {
namespace storage {

void saveListOfIntsToFile(const string& fName, unique_ptr<uint32_t[]>& data, uint32_t listSize);

uint32_t readListOfIntsFromFile(unique_ptr<uint32_t[]>& data, const string& fName);

} // namespace storage
} // namespace graphflow
