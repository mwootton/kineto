/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include "GenericActivityProfiler.h"

namespace KINETO_NAMESPACE {

class RpdActivityProfiler : public GenericActivityProfiler {
 public:
  explicit RpdActivityProfiler(bool cpuOnly);
  RpdActivityProfiler(const RpdActivityProfiler&) = delete;
  RpdActivityProfiler& operator=(const RpdActivityProfiler&) = delete;
  ~RpdActivityProfiler() override;

 protected:
  void logGpuVersions() override;
  void setMaxGpuBufferSize(int64_t size) override;
  void enableGpuTracing() override;
  void disableGpuTracing() override;
  void clearGpuActivities() override;
  bool isGpuCollectionStopped() const override;
  void processGpuActivities(ActivityLogger& logger) override;
  void synchronizeGpuDevice() override;
  void pushCorrelationIdImpl(uint64_t id, CorrelationFlowType type) override;
  void popCorrelationIdImpl(CorrelationFlowType type) override;
  void onResetTraceData() override;
  void onFinalizeTrace(const Config& config, ActivityLogger& logger) override;

 private:
  struct RpdActivityProfilerPrivate;
  std::unique_ptr<RpdActivityProfilerPrivate> d_;
};

} // namespace KINETO_NAMESPACE
