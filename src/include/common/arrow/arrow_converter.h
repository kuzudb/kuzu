#pragma once

#include <string>
#include <vector>

#include "common/arrow/arrow.h"
#include "common/arrow/arrow_nullmask_tree.h"
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
    static void toArrowArray(main::QueryResult& queryResult, ArrowArray* out_array,
        std::int64_t chunkSize);

    static common::LogicalType fromArrowSchema(const ArrowSchema* schema);
    static void fromArrowArray(const ArrowSchema* schema, const ArrowArray* array,
        ValueVector& outputVector, ArrowNullMaskTree* mask, uint64_t srcOffset, uint64_t dstOffset,
        uint64_t count);
    static void fromArrowArray(const ArrowSchema* schema, const ArrowArray* array,
        ValueVector& outputVector);

private:
    static void initializeChild(ArrowSchema& child, const std::string& name = "");
    static void setArrowFormatForStruct(ArrowSchemaHolder& rootHolder, ArrowSchema& child,
        const main::DataTypeInfo& typeInfo);
    static void setArrowFormat(ArrowSchemaHolder& rootHolder, ArrowSchema& child,
        const main::DataTypeInfo& typeInfo);
};

} // namespace common
} // namespace kuzu
