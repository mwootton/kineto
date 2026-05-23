/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "RpdActivityProfiler.h"

#include <dlfcn.h>
#include <fmt/format.h>
#include <sqlite3.h>

#include <algorithm>
#include <cstdlib>
#include <deque>
#include <map>
#include <mutex>
#include <vector>

#include "ActivityBuffers.h"
#include "Config.h"
#include "Demangle.h"
#include "DeviceUtil.h"
#include "GenericTraceActivity.h"
#include "Logger.h"
#include "RpdUtil.h"
#include "ThreadUtil.h"
#include "output_base.h"

using namespace std::chrono;

namespace KINETO_NAMESPACE {

namespace {

thread_local std::deque<uint64_t> t_externalIds[2];

struct CorrelationInterval {
  int64_t startNs;
  int64_t endNs;
  uint64_t externalId;
};

} // namespace

struct RpdActivityProfiler::RpdActivityProfilerPrivate {
  void* handle{nullptr};
  bool available{false};
  std::once_flag initFlag;

  using VoidFn = void (*)();
  using SetConfigFn = void (*)(const char*, const char*);
  using GetConnectionFn = sqlite3* (*)();
  using ResetStorageFn = void (*)();

  VoidFn rpdstart{nullptr};
  VoidFn rpdstop{nullptr};
  VoidFn rpdflush{nullptr};
  SetConfigFn rpd_setConfig{nullptr};
  GetConnectionFn rpd_getConnection{nullptr};
  ResetStorageFn rpd_resetStorage{nullptr};

  int64_t captureStartMonoNs{0};
  int64_t captureEndMonoNs{0};
  bool tracingActive{false};
  int sessionCounter{0};

  struct CorrelationEvent {
    int64_t monoNs;
    int32_t tid;
    uint64_t externalId;
    int domain;
    bool isPush;
  };
  std::vector<CorrelationEvent> correlationLog;
  std::mutex correlationMutex;

  void ensureLoaded() {
    if (dlopen("librpd_embedded.so", RTLD_NOW | RTLD_NOLOAD)) {
      LOG(WARNING) << "librpd_embedded.so already loaded by another component";
      return;
    }
    handle = dlopen("librpd_embedded.so", RTLD_NOW);
    if (!handle) {
      LOG(INFO) << "librpd_embedded.so not available";
      return;
    }

    rpdstart = reinterpret_cast<VoidFn>(dlsym(handle, "rpdstart"));
    rpdstop = reinterpret_cast<VoidFn>(dlsym(handle, "rpdstop"));
    rpdflush = reinterpret_cast<VoidFn>(dlsym(handle, "rpdflush"));
    rpd_setConfig =
        reinterpret_cast<SetConfigFn>(dlsym(handle, "rpd_setConfig"));
    rpd_getConnection =
        reinterpret_cast<GetConnectionFn>(dlsym(handle, "rpd_getConnection"));
    rpd_resetStorage =
        reinterpret_cast<ResetStorageFn>(dlsym(handle, "rpd_resetStorage"));

    if (!rpdstart || !rpdstop || !rpdflush || !rpd_setConfig ||
        !rpd_getConnection || !rpd_resetStorage) {
      LOG(WARNING) << "librpd_embedded.so loaded but missing symbols";
      return;
    }

    rpd_setConfig("autostart", "0");
    rpd_setConfig("directwrite", "1");
    rpd_setConfig("quiet", "1");

    available = true;
    LOG(INFO) << "RPD embedded tracer loaded";
  }

  // ---- Correlation interval logic ----

  using IntervalMap =
      std::map<int32_t, std::vector<CorrelationInterval>>;

  void buildCorrelationIntervals(
      IntervalMap& defaultIntervals,
      IntervalMap& userIntervals) {
    std::lock_guard<std::mutex> guard(correlationMutex);

    std::sort(
        correlationLog.begin(),
        correlationLog.end(),
        [](const CorrelationEvent& a, const CorrelationEvent& b) {
          return std::tie(a.tid, a.monoNs) < std::tie(b.tid, b.monoNs);
        });

    std::map<int32_t, std::deque<std::pair<int64_t, uint64_t>>> stacks[2];

    for (const auto& ev : correlationLog) {
      auto& stack = stacks[ev.domain][ev.tid];
      auto& intervals =
          (ev.domain == 0) ? defaultIntervals : userIntervals;

      if (ev.isPush) {
        stack.push_back({ev.monoNs, ev.externalId});
      } else if (!stack.empty()) {
        auto [startNs, extId] = stack.back();
        stack.pop_back();
        intervals[ev.tid].push_back({startNs, ev.monoNs, extId});
      }
    }

    for (int d = 0; d < 2; ++d) {
      auto& intervals = (d == 0) ? defaultIntervals : userIntervals;
      for (auto& [tid, stack] : stacks[d]) {
        while (!stack.empty()) {
          auto [startNs, extId] = stack.back();
          stack.pop_back();
          intervals[tid].push_back({startNs, captureEndMonoNs, extId});
        }
      }
    }

    for (auto* map : {&defaultIntervals, &userIntervals}) {
      for (auto& [tid, ivs] : *map) {
        std::sort(
            ivs.begin(),
            ivs.end(),
            [](const CorrelationInterval& a, const CorrelationInterval& b) {
              return a.startNs < b.startNs;
            });
      }
    }
  }

  static uint64_t findCorrelation(
      const IntervalMap& intervals,
      int32_t tid,
      int64_t timestampNs) {
    auto tidIt = intervals.find(tid);
    if (tidIt == intervals.end()) {
      return 0;
    }
    const auto& ivs = tidIt->second;
    auto it = std::upper_bound(
        ivs.begin(),
        ivs.end(),
        timestampNs,
        [](int64_t ts, const CorrelationInterval& iv) {
          return ts < iv.startNs;
        });
    if (it != ivs.begin()) {
      --it;
      if (timestampNs >= it->startNs && timestampNs <= it->endNs) {
        return it->externalId;
      }
    }
    return 0;
  }
};

// ---- Constructor / Destructor ----

RpdActivityProfiler::RpdActivityProfiler(bool cpuOnly)
    : GenericActivityProfiler(cpuOnly), d_(std::make_unique<RpdActivityProfilerPrivate>()) {
  if (isGpuAvailable()) {
    logGpuVersions();
  }
}

RpdActivityProfiler::~RpdActivityProfiler() = default;

// ---- Simple overrides ----

void RpdActivityProfiler::logGpuVersions() {
  std::call_once(d_->initFlag, &RpdActivityProfilerPrivate::ensureLoaded, d_.get());
  if (!d_->available) {
    return;
  }
  int hipRuntimeVersion = 0;
  int hipDriverVersion = 0;
  CUDA_CALL(hipRuntimeGetVersion(&hipRuntimeVersion));
  CUDA_CALL(hipDriverGetVersion(&hipDriverVersion));
  LOG(INFO) << "HIP versions. Profiler: rpd_tracer"
            << "; Runtime: " << hipRuntimeVersion
            << "; Driver: " << hipDriverVersion;

  LOGGER_OBSERVER_ADD_METADATA("profiler_backend", "rpd_tracer");
  LOGGER_OBSERVER_ADD_METADATA(
      "hip_runtime_version", std::to_string(hipRuntimeVersion));
  LOGGER_OBSERVER_ADD_METADATA(
      "hip_driver_version", std::to_string(hipDriverVersion));
  addVersionMetadata("profiler_backend", "\"rpd_tracer\"");
  addVersionMetadata(
      "hip_runtime_version", std::to_string(hipRuntimeVersion));
  addVersionMetadata(
      "hip_driver_version", std::to_string(hipDriverVersion));
}

void RpdActivityProfiler::setMaxGpuBufferSize(int64_t /*size*/) {
}

void RpdActivityProfiler::enableGpuTracing() {
  std::call_once(d_->initFlag, &RpdActivityProfilerPrivate::ensureLoaded, d_.get());
  if (!d_->available) {
    return;
  }

  // Configure RPD before starting
  const auto& filename = config().rpdFilename();
  if (filename != ":memory:" && d_->sessionCounter > 0) {
    auto dot = filename.rfind('.');
    std::string sessionFilename = (dot != std::string::npos)
        ? filename.substr(0, dot) + "_" +
              std::to_string(d_->sessionCounter) + filename.substr(dot)
        : filename + "_" + std::to_string(d_->sessionCounter);
    d_->rpd_setConfig("filename", sessionFilename.c_str());
  } else {
    d_->rpd_setConfig("filename", filename.c_str());
  }
  ++d_->sessionCounter;

  d_->rpd_setConfig("datasources_exclude",
      "RoctxDataSource,NvtxDataSource,RlogDataSource,RocmSmiDataSource");

  const auto& priority = config().rpdDatasourcePriority();
  if (!priority.empty()) {
    d_->rpd_setConfig("datasources_priority", priority.c_str());
  }

  const auto& ds = config().rpdDatasource();
  if (!ds.empty()) {
    d_->rpd_setConfig("datasources_explicit", ds.c_str());
  }

  d_->rpd_resetStorage();

  d_->captureStartMonoNs = monoTimeNs();
  d_->rpdstart();
  d_->tracingActive = true;
  LOG(INFO) << "RPD tracing started";
}

void RpdActivityProfiler::disableGpuTracing() {
  if (!d_->available || !d_->tracingActive) {
    return;
  }
  d_->rpdstop();
  d_->rpdflush();
  d_->captureEndMonoNs = monoTimeNs();
  d_->tracingActive = false;
  LOG(INFO) << "RPD tracing stopped, duration: "
            << (d_->captureEndMonoNs - d_->captureStartMonoNs) / 1000000
            << "ms";
}

void RpdActivityProfiler::clearGpuActivities() {
  d_->captureStartMonoNs = 0;
  d_->captureEndMonoNs = 0;
  std::lock_guard<std::mutex> guard(d_->correlationMutex);
  d_->correlationLog.clear();
}

bool RpdActivityProfiler::isGpuCollectionStopped() const {
  return false;
}

void RpdActivityProfiler::synchronizeGpuDevice() {
  CUDA_CALL(hipDeviceSynchronize());
  if (d_->available && d_->tracingActive) {
    d_->rpdflush();
  }
}

void RpdActivityProfiler::pushCorrelationIdImpl(
    uint64_t id,
    CorrelationFlowType type) {
  int idx = (type == CorrelationFlowType::User) ? 1 : 0;
  t_externalIds[idx].push_back(id);

  std::lock_guard<std::mutex> guard(d_->correlationMutex);
  d_->correlationLog.push_back(
      {monoTimeNs(), systemThreadId(), id, idx, true});
}

void RpdActivityProfiler::popCorrelationIdImpl(CorrelationFlowType type) {
  int idx = (type == CorrelationFlowType::User) ? 1 : 0;
  if (!t_externalIds[idx].empty()) {
    t_externalIds[idx].pop_back();
  }

  std::lock_guard<std::mutex> guard(d_->correlationMutex);
  d_->correlationLog.push_back(
      {monoTimeNs(), systemThreadId(), 0, idx, false});
}

void RpdActivityProfiler::onResetTraceData() {
  {
    std::lock_guard<std::mutex> guard(d_->correlationMutex);
    d_->correlationLog.clear();
  }
  d_->captureStartMonoNs = 0;
  d_->captureEndMonoNs = 0;
  d_->tracingActive = false;
}

void RpdActivityProfiler::onFinalizeTrace(
    [[maybe_unused]] const Config& config,
    [[maybe_unused]] ActivityLogger& logger) {
}

// ---- Core: processGpuActivities ----

void RpdActivityProfiler::processGpuActivities(ActivityLogger& logger) {
  if (!d_->available || captureWindowStartTime_ == 0) {
    return;
  }

  VLOG(0) << "Processing RPD GPU activity buffers";

  RpdActivityProfilerPrivate::IntervalMap defaultIntervals;
  RpdActivityProfilerPrivate::IntervalMap userIntervals;
  d_->buildCorrelationIntervals(defaultIntervals, userIntervals);

  SqliteConnection db(d_->rpd_getConnection());
  if (!db) {
    LOG(WARNING) << "Failed to get RPD database connection";
    return;
  }

  if (!traceBuffers_) {
    traceBuffers_ = std::make_unique<ActivityBuffers>();
  }

  const auto& actTypes = derivedConfig_->profileActivityTypes();
  bool wantRuntime = actTypes.count(ActivityType::CUDA_RUNTIME);
  bool wantKernels = actTypes.count(ActivityType::CONCURRENT_KERNEL);
  bool wantMemcpy = actTypes.count(ActivityType::GPU_MEMCPY);
  bool wantGpuOps = wantKernels || wantMemcpy;

  int runtimeCount = 0;
  int gpuCount = 0;

  // HIP runtime activities
  if (wantRuntime) {
    SqliteStmt stmt(
        db.get(),
        "SELECT a.id, a.pid, a.tid, a.start, a.end, "
        "       sName.string AS apiName "
        "FROM rocpd_api a "
        "JOIN rocpd_string sName ON a.apiName_id = sName.id "
        "WHERE a.domain_id IN "
        "  (SELECT id FROM rocpd_string WHERE string = 'hip') "
        "ORDER BY a.start");

    while (stmt.step()) {
      int64_t apiId = stmt.colInt64(0);
      int32_t pid = stmt.colInt(1);
      int32_t tid = stmt.colInt(2);
      int64_t startNs = stmt.colInt64(3);
      int64_t endNs = stmt.colInt64(4);
      const char* apiName = stmt.colText(5);

      uint64_t extId =
          RpdActivityProfilerPrivate::findCorrelation(defaultIntervals, tid, startNs);
      if (extId != 0) {
        cpuCorrelationMap_[apiId] = extId;
      }
      uint64_t userExtId =
          RpdActivityProfilerPrivate::findCorrelation(userIntervals, tid, startNs);
      if (userExtId != 0) {
        userCorrelationMap_[apiId] = userExtId;
      }

      GenericTraceActivity act;
      act.startTime = startNs;
      act.endTime = endNs;
      act.id = static_cast<int32_t>(apiId);
      act.device = pid;
      act.resource = tid;
      act.threadId = tid;
      act.activityType = ActivityType::CUDA_RUNTIME;
      act.activityName = apiName;
      act.flow.type = kLinkAsyncCpuGpu;
      act.flow.id = static_cast<uint32_t>(apiId);
      act.flow.start = true;

      const ITraceActivity* linked =
          linkedActivity(apiId, cpuCorrelationMap_);
      act.linked = linked;

      const auto& stored = traceBuffers_->addActivityWrapper(act);
      checkTimestampOrder(&stored);
      if (!outOfRange(stored)) {
        stored.log(logger);
        setGpuActivityPresent(true);
      }
      ++runtimeCount;
    }
  }

  // GPU operations (kernels, memcpy)
  if (wantGpuOps) {
    SqliteStmt stmt(
        db.get(),
        "SELECT o.id, o.gpuId, o.queueId, o.start, o.end, "
        "       sDesc.string AS description, "
        "       (CASE WHEN o.opType_id IN "
        "         (SELECT id FROM rocpd_string WHERE string = 'Memcpy') "
        "         THEN 1 ELSE 0 END) AS isMemcpy, "
        "       ao.api_id, "
        "       k.gridX, k.gridY, k.gridZ, "
        "       k.workgroupX, k.workgroupY, k.workgroupZ, "
        "       k.groupSegmentSize, "
        "       sk.string AS kernelName, "
        "       c.size AS copySize, c.kind AS copyKind, "
        "       c.dstDevice, c.srcDevice "
        "FROM rocpd_op o "
        "JOIN rocpd_string sDesc ON o.description_id = sDesc.id "
        "JOIN rocpd_api_ops ao ON ao.op_id = o.id "
        "LEFT JOIN rocpd_kernelapi k ON k.api_ptr_id = ao.api_id "
        "LEFT JOIN rocpd_string sk ON k.kernelName_id = sk.id "
        "LEFT JOIN rocpd_copyapi c ON c.api_ptr_id = ao.api_id "
        "ORDER BY o.start");

    while (stmt.step()) {
      int gpuId = stmt.colInt(1);
      int queueId = stmt.colInt(2);
      int64_t startNs = stmt.colInt64(3);
      int64_t endNs = stmt.colInt64(4);
      const char* description = stmt.colText(5);
      bool isMemcpy = stmt.colInt(6) != 0;
      int64_t apiId = stmt.colInt64(7);
      bool hasKernel = !stmt.colIsNull(8);
      int gridX = stmt.colInt(8);
      int gridY = stmt.colInt(9);
      int gridZ = stmt.colInt(10);
      int wgX = stmt.colInt(11);
      int wgY = stmt.colInt(12);
      int wgZ = stmt.colInt(13);
      int groupSegmentSize = stmt.colInt(14);
      const char* kernelName = stmt.colText(15);
      bool hasCopy = !stmt.colIsNull(16);
      int64_t copySize = stmt.colInt64(16);
      int dstDevice = stmt.colInt(18);
      int srcDevice = stmt.colInt(19);

      int64_t durationNs = endNs - startNs;

      ActivityType actType = isMemcpy
          ? ActivityType::GPU_MEMCPY
          : ActivityType::CONCURRENT_KERNEL;

      if ((isMemcpy && !wantMemcpy) || (!isMemcpy && !wantKernels)) {
        continue;
      }

      std::string name;
      if (hasKernel && kernelName[0] != '\0') {
        name = demangle(kernelName);
      } else if (description[0] != '\0') {
        name = description;
      } else if (isMemcpy && hasCopy) {
        name = fmt::format("Memcpy ({} bytes)", copySize);
      } else if (isMemcpy) {
        name = "Memcpy";
      } else {
        name = "GPU op";
      }

      GenericTraceActivity act;
      act.startTime = startNs;
      act.endTime = endNs;
      act.id = static_cast<int32_t>(apiId);
      act.device = gpuId;
      act.resource = queueId;
      act.activityType = actType;
      act.activityName = std::move(name);
      act.flow.type = kLinkAsyncCpuGpu;
      act.flow.id = static_cast<uint32_t>(apiId);
      act.flow.start = false;

      const ITraceActivity* linked =
          linkedActivity(apiId, cpuCorrelationMap_);
      act.linked = linked;

      if (hasKernel) {
        act.addMetadata("grid",
            fmt::format("[{}, {}, {}]", gridX, gridY, gridZ));
        act.addMetadata("block",
            fmt::format("[{}, {}, {}]", wgX, wgY, wgZ));
        act.addMetadata("shared memory", groupSegmentSize);
      }

      if (hasCopy) {
        act.addMetadata("bytes", copySize);
        act.addMetadata(
            "memory bandwidth (GB/s)", bandwidth(copySize, durationNs));
        act.addMetadata("src_device", srcDevice);
        act.addMetadata("dst_device", dstDevice);
      }

      act.addMetadata("device", gpuId);
      act.addMetadata("stream", queueId);
      act.addMetadataQuoted("correlation", std::to_string(apiId));

      const auto& stored = traceBuffers_->addActivityWrapper(act);
      handleGpuActivity(stored, &logger);
      ++gpuCount;
    }
  }

  LOG(INFO) << "Processed " << runtimeCount << " RPD runtime + "
            << gpuCount << " GPU records";
  LOGGER_OBSERVER_ADD_EVENT_COUNT(runtimeCount + gpuCount);
}

} // namespace KINETO_NAMESPACE
