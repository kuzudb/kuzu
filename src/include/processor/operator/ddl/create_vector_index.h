#pragma once

#include "common/vector_index/vector_index_config.h"
#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

class CreateVectorIndex : public DDL {
public:
    CreateVectorIndex(std::string tableName, std::string propertyName, common::table_id_t tableId,
        common::property_id_t propertyId, int dim, const common::VectorIndexConfig config,
        const DataPos& outputPos, uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : DDL{PhysicalOperatorType::CREATE_VECTOR_INDEX, outputPos, id, std::move(printInfo)},
          tableName{tableName}, propertyName{propertyName}, tableId{tableId},
          propertyId{propertyId}, dim{dim}, config{config} {};

    void executeDDLInternal(ExecutionContext* context) final;

    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CreateVectorIndex>(tableName, propertyName, tableId, propertyId,
            dim, config, outputPos, id, printInfo->copy());
    }

private:
    std::string tableName;
    std::string propertyName;
    common::table_id_t tableId;
    common::property_id_t propertyId;
    int dim;
    common::VectorIndexConfig config;
};

} // namespace processor
} // namespace kuzu
