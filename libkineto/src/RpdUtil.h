/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fmt/format.h>
#include <sqlite3.h>
#include <time.h>

#include <cstdint>
#include <string>

#include "Logger.h"

namespace KINETO_NAMESPACE {

// Clock helpers

inline int64_t monoTimeNs() {
  struct timespec ts {};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<int64_t>(ts.tv_sec) * 1000000000 +
      static_cast<int64_t>(ts.tv_nsec);
}

inline std::string bandwidth(int64_t bytes, int64_t durationNs) {
  return durationNs == 0
      ? "\"N/A\""
      : fmt::format("{}", bytes * 1.0 / durationNs);
}

// SQLite wrappers

class SqliteConnection {
 public:
  explicit SqliteConnection(sqlite3* db) : db_(db) {}
  ~SqliteConnection() {
    if (db_) {
      sqlite3_close(db_);
    }
  }
  SqliteConnection(const SqliteConnection&) = delete;
  SqliteConnection& operator=(const SqliteConnection&) = delete;

  sqlite3* get() const { return db_; }
  explicit operator bool() const { return db_ != nullptr; }

 private:
  sqlite3* db_{nullptr};
};

class SqliteStmt {
 public:
  SqliteStmt(sqlite3* db, const char* sql) {
    sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr);
  }
  ~SqliteStmt() {
    if (stmt_) {
      sqlite3_finalize(stmt_);
    }
  }
  SqliteStmt(const SqliteStmt&) = delete;
  SqliteStmt& operator=(const SqliteStmt&) = delete;

  sqlite3_stmt* get() const { return stmt_; }

  void bindInt64(int idx, int64_t val) {
    sqlite3_bind_int64(stmt_, idx, val);
  }

  bool step() { return sqlite3_step(stmt_) == SQLITE_ROW; }

  int64_t colInt64(int idx) { return sqlite3_column_int64(stmt_, idx); }
  int colInt(int idx) { return sqlite3_column_int(stmt_, idx); }
  const char* colText(int idx) {
    auto* t = reinterpret_cast<const char*>(sqlite3_column_text(stmt_, idx));
    return t ? t : "";
  }
  bool colIsNull(int idx) {
    return sqlite3_column_type(stmt_, idx) == SQLITE_NULL;
  }

 private:
  sqlite3_stmt* stmt_{nullptr};
};

} // namespace KINETO_NAMESPACE
