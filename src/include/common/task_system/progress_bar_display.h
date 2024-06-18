#pragma once

#include <stdint.h>
#include <string>

namespace kuzu {
namespace common {

/**
 * @brief Interface for displaying progress of a pipeline and a query.
 */
class ProgressBarDisplay {
public:
    // Update the progress of the pipeline and the number of finished pipelines
    virtual void updateProgress(std::string id, double newPipelineProgress, uint32_t newNumPipelinesFinished) = 0;

    // Finish the progress display
    virtual void finishProgress(std::string id) = 0;

    void setNumPipelines(std::string id, uint32_t newNumPipelines) { numPipelines = newNumPipelines; };

protected:
    double pipelineProgress;
    uint32_t numPipelines;
    uint32_t numPipelinesFinished;
};

} // namespace common
} // namespace kuzu
