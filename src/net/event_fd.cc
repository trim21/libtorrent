#include "config.h"

#include "net/event_fd.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <unistd.h>

#ifdef USE_EPOLL
#include <sys/eventfd.h>
#endif

#include "torrent/exceptions.h"
#include "torrent/runtime/socket_manager.h"
#include "torrent/system/poll.h"

namespace torrent::net {

// ── diagnostic: track which thread sends signals to which eventfd ─────

static std::atomic<uint64_t> g_signal_count;
static thread_local uint64_t t_signal_count;

static std::atomic<int> g_signal_logging;  // prevent concurrent fprintf

static void
diag_log_signal(const char* who_sent) {
  auto count = g_signal_count.fetch_add(1) + 1;
  auto tcnt  = ++t_signal_count;

  if (tcnt <= 50 || tcnt % 5000 == 0) {
    int expected{};
    if (!g_signal_logging.compare_exchange_strong(expected, 1))
      return;

    FILE* fp = fopen("/tmp/eventfd_diag.log", "a");
    if (fp) {
      auto now = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
      fprintf(fp, "[send_signal] total=%lu tid=%s local=%lu ts=%ld\n",
              (unsigned long)count, who_sent, (unsigned long)tcnt, (long)now);
      fclose(fp);
    }
    g_signal_logging.store(0);
  }
}

void
EventFd::add_to_poll() {
  errno = 0;

#ifdef USE_EPOLL
  set_file_descriptor(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));

  m_safe_fd = file_descriptor();
#endif

  if (file_descriptor() == -1)
    throw internal_error("EventFd::add_to_poll() eventfd failed: " + std::string(std::strerror(errno)));

  runtime::socket_manager()->register_event_or_throw(this, runtime::category_internal, [this]() {
      this_thread::poll()->open(this);
      this_thread::poll()->insert_read(this);
    });
}

void
EventFd::remove_from_poll(system::Poll* poll) {
  if (!is_open())
    return;

  m_safe_fd = -1;

  runtime::socket_manager()->unregister_event_or_throw(this, [this, poll]() {
      poll->remove_and_close(this);
    });
}

// Poll uses a state flag to ensure we only send a signal once per interrupt.
void
EventFd::send_signal() {
  diag_log_signal(this_thread::thread_name());

  uint64_t value = 1;

  while (true) {
    switch (::write(m_safe_fd.load(), &value, sizeof(value))) {
    case sizeof(value):
      return;

    case 0:
      throw internal_error("EventFd::send_signal() write returned 0: " + this_thread::thread_name_str());

    case -1:
      if (errno == EINTR)
        continue;

      // Only happens if the eventfd counter is at its maximum value, so it's already interrupting.
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        return;

      // Ignore spurious interrupt attempts right before/after threads enter their event loop.
      if (errno == EBADF && m_safe_fd == -1)
        return;

      throw internal_error("EventFd::send_signal() write failed: " + this_thread::thread_name_str() + " : " + std::string(std::strerror(errno)));

    default:
      throw internal_error("EventFd::send_signal() write returned unexpected value: " + this_thread::thread_name_str());
    }
  }
}

void
EventFd::event_read() {
  uint64_t value;

  while (true) {
    switch (::read(file_descriptor(), &value, sizeof(value))) {
    case sizeof(value):
      return;

    case 0:
      throw internal_error("EventFd::event_read() read returned 0: " + this_thread::thread_name_str());

    case -1:
      if (errno == EINTR)
        continue;

      if (errno == EAGAIN || errno == EWOULDBLOCK)
        return;

      throw internal_error("EventFd::event_read() read failed: " + std::string(std::strerror(errno)));

    default:
      throw internal_error("EventFd::event_read() read returned unexpected value: " + this_thread::thread_name_str());
    }
  }
}

void
EventFd::event_write() {
  throw internal_error("EventFd::event_write() called but EventFd does not support writing.");
}

void
EventFd::event_error() {
  throw internal_error("EventFd::event_error() called but EventFd does not support error events.");
}

} // namespace torrent::net

// ── diagnostic: cross-thread callback tracking ────────────────────────

#include <atomic>
#include <cstdio>
#include <cstring>

struct diag_cb_bucket {
  std::atomic<uint64_t> count;
  char                  target[32];
  char                  caller[32];
  bool                  intr;
};

static constexpr int kDiagCbBuckets = 64;
static diag_cb_bucket g_cb_buckets[kDiagCbBuckets];
static std::atomic<int> g_cb_next_bucket;

void
__diag_track_callback(const char* target, const char* caller, bool is_intr) {
  int idx{};
  for (int i = 0; i < kDiagCbBuckets; i++) {
    if (g_cb_buckets[i].intr == is_intr &&
        std::strcmp(g_cb_buckets[i].target, target) == 0 &&
        std::strcmp(g_cb_buckets[i].caller, caller) == 0) {
      g_cb_buckets[i].count.fetch_add(1);
      return;
    }
    if (g_cb_buckets[i].target[0] == '\0' && idx == 0) idx = i;
  }

  if (idx == 0) idx = g_cb_next_bucket.fetch_add(1) % kDiagCbBuckets;
  std::strncpy(g_cb_buckets[idx].target, target, 31);
  std::strncpy(g_cb_buckets[idx].caller, caller, 31);
  g_cb_buckets[idx].intr = is_intr;
  g_cb_buckets[idx].count.store(1);

  FILE* fp = fopen("/tmp/eventfd_diag.log", "a");
  if (fp) {
    fprintf(fp, "[callback_new] target=%s caller=%s intr=%d\n", target, caller, (int)is_intr);
    fclose(fp);
  }
}

void
__diag_dump_callbacks() {
  FILE* fp = fopen("/tmp/eventfd_diag.log", "a");
  if (!fp) return;
  fprintf(fp, "=== callback snapshot ===\n");
  for (int i = 0; i < kDiagCbBuckets; i++) {
    auto c = g_cb_buckets[i].count.load();
    if (c == 0) continue;
    fprintf(fp, "  [cb#%d] target=%s caller=%s intr=%d count=%lu\n",
            i, g_cb_buckets[i].target, g_cb_buckets[i].caller,
            (int)g_cb_buckets[i].intr, (unsigned long)c);
  }
  fprintf(fp, "========================\n");
  fclose(fp);
}
