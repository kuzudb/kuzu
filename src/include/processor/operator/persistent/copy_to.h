#pragma once

#include "processor/operator/sink.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

struct CopyToInfo {
    std::vector<std::string> names;
    std::vector<DataPos> dataPoses;
    std::string fileName;

    CopyToInfo(std::vector<std::string> names, std::vector<DataPos> dataPoses, std::string fileName)
        : names{std::move(names)}, dataPoses{std::move(dataPoses)}, fileName{std::move(fileName)} {}

    virtual ~CopyToInfo() = default;

    virtual std::unique_ptr<CopyToInfo> copy() = 0;
};

class CopyToSharedState;

class CopyToLocalState {
public:
    virtual ~CopyToLocalState() = default;

    virtual void init(CopyToInfo* info, storage::MemoryManager* mm, ResultSet* resultSet) = 0;

    virtual void sink(CopyToSharedState* sharedState, CopyToInfo* info) = 0;

    virtual void finalize(CopyToSharedState* sharedState) = 0;
};

class CopyToSharedState {
public:
    virtual ~CopyToSharedState() = default;

    virtual void init(CopyToInfo* info, storage::MemoryManager* mm) = 0;

    virtual void finalize() = 0;
};

class CopyTo : public Sink {
public:
    CopyTo(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<CopyToInfo> info, std::unique_ptr<CopyToLocalState> localState,
        std::shared_ptr<CopyToSharedState> sharedState, PhysicalOperatorType opType,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), opType, std::move(child), id, paramsString},
          info{std::move(info)}, localState{std::move(localState)}, sharedState{
                                                                        std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    void initGlobalStateInternal(ExecutionContext* context) final;

    void finalize(ExecutionContext* context) final;

    void executeInternal(processor::ExecutionContext* context) final;

protected:
    std::unique_ptr<CopyToInfo> info;
    std::unique_ptr<CopyToLocalState> localState;
    std::shared_ptr<CopyToSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
