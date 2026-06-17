#include "config.h"

#include "net/event_fd.h"

#include <atomic>
#include <cstring>
#include <unistd.h>

#ifdef USE_EPOLL
#include <sys/eventfd.h>
#endif

#include "torrent/exceptions.h"
#include "torrent/runtime/socket_manager.h"
#include "torrent/system/poll.h"

namespace torrent::net {

// ── diagnostic counters (zero hot-path I/O) ───────────────────────────

static constexpr int kSigSlots = 16;

struct SigSlot {
  std::atomic<uint64_t> count;
  char                  name[32];
};

static SigSlot g_sig_slots[kSigSlots];
static std::atomic<int> g_sig_next_slot;
static thread_local int t_sig_slot = -1;

static int find_or_create_slot(const char* name) {
  if (t_sig_slot >= 0 && std::strcmp(g_sig_slots[t_sig_slot].name, name) == 0)
    return t_sig_slot;

  for (int i = 0; i < kSigSlots; i++) {
    if (std::strcmp(g_sig_slots[i].name, name) == 0) {
      t_sig_slot = i;
      return i;
    }
  }

  int idx = g_sig_next_slot.fetch_add(1, std::memory_order_relaxed) % kSigSlots;
  std::strncpy(g_sig_slots[idx].name, name, 31);
  g_sig_slots[idx].name[31] = '\0';
  g_sig_slots[idx].count.store(0, std::memory_order_relaxed);
  t_sig_slot = idx;
  return idx;
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
  int slot = find_or_create_slot(this_thread::thread_name());
  g_sig_slots[slot].count.fetch_add(1, std::memory_order_relaxed);

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

#include <cstdio>

__attribute__((visibility("default"))) extern "C" void
__diag_format_signal_counts(char* buf, int sz) {
  using namespace torrent::net;
  int pos = 0;
  for (int i = 0; i < kSigSlots; i++) {
    if (g_sig_slots[i].name[0]) {
      auto c = g_sig_slots[i].count.load(std::memory_order_relaxed);
      if (c == 0) continue;
      int n = snprintf(buf + pos, sz - pos, "%s=%lu ",
                       g_sig_slots[i].name, (unsigned long)c);
      if (n > 0 && n < sz - pos)
        pos += n;
    }
  }
  if (pos == 0 && sz > 0) buf[0] = '\0';
}
