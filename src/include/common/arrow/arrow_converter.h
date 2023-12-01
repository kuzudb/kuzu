#pragma once

#include <string>
#include <vector>

#include "common/arrow/arrow.h"
#include "main/query_result.h"

struct ArrowSchema;

namespace kuzu {
namespace common {

struct ArrowSchemaHolder {
    std::vector<ArrowSchema> children;
    std::vector<ArrowSchema*> childrenPtrs;
    std::vector<std::vector<ArrowSchema>> nestedChildren;
    std::vector<std::vector<ArrowSchema*>> nestedChildrenPtr;
    std::vector<std::unique_ptr<char[]>> ownedTypeNames;
};

struct ArrowConverter {
public:
    static std::unique_ptr<ArrowSchema> toArrowSchema(
        const std::vector<std::unique_ptr<main::DataTypeInfo>>& typesInfo);
    static void toArrowArray(
        main::QueryResult& queryResult, ArrowArray* out_array, std::int64_t chunkSize);

private:
    static void initializeChild(ArrowSchema& child, const std::string& name = "");
    static void setArrowFormatForStruct(
        ArrowSchemaHolder& rootHolder, ArrowSchema& child, const main::DataTypeInfo& typeInfo);
    static void setArrowFormat(
        ArrowSchemaHolder& rootHolder, ArrowSchema& child, const main::DataTypeInfo& typeInfo);
};

} // namespace common
} // namespace kuzu
