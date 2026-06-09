#include "config.h"

#include "torrent/utils/scheduler.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdio>

#include "torrent/exceptions.h"
#include "torrent/utils/chrono.h"

#include <cinttypes>

namespace torrent::utils {

static constexpr auto compare = [](const SchedulerEntry* a, const SchedulerEntry* b) {
  return a->time() > b->time();
};

// Sift element at `pos` upward in the heap [first, first+pos+1)
static void sift_up(Scheduler::iterator first, size_t pos) {
  auto value = std::move(first[pos]);
  while (pos > 0) {
    size_t parent = (pos - 1) / 2;
    if (!compare(value, first[parent]))
      break;
    first[pos] = std::move(first[parent]);
    pos = parent;
  }
  first[pos] = std::move(value);
}

// Sift element at `pos` downward in the heap [first, first+size)
static void sift_down(Scheduler::iterator first, size_t size, size_t pos) {
  auto value = std::move(first[pos]);
  while (true) {
    size_t child = 2 * pos + 1;
    if (child >= size)
      break;
    if (child + 1 < size && compare(first[child + 1], first[child]))
      child++;
    if (!compare(first[child], value))
      break;
    first[pos] = std::move(first[child]);
    pos = child;
  }
  first[pos] = std::move(value);
}

SchedulerEntry::~SchedulerEntry() {
  assert(!is_scheduled() && "SchedulerEntry::~SchedulerEntry() called on a scheduled item.");
  assert(m_time == time_type{} && "SchedulerEntry::~SchedulerEntry() called on an item with a time.");

  m_slot = nullptr;
  m_scheduler = nullptr;
  m_time = time_type{};
}


inline void
Scheduler::make_heap() {
  std::make_heap(begin(), end(), compare);
}

inline void
Scheduler::push_heap() {
  std::push_heap(begin(), end(), compare);
}

Scheduler::time_type
Scheduler::next_timeout() const {
  assert(!empty());

  return std::max(front()->time() - m_cached_time, Scheduler::time_type());
}

// We can't make erase/update part of SchedulerItem in case another thread tries to call the
// scheduler, which is not thread-safe.
void
Scheduler::erase(SchedulerEntry* entry) {
  assert(m_thread_id == std::thread::id() || m_thread_id == std::this_thread::get_id());

  if (!entry->is_scheduled())
    return;

  // Check is_valid() after is_schedulerd() so that it is safe to call
  // erase on untouched instances.
  if (!entry->is_valid())
    throw torrent::internal_error("Scheduler::erase(...) called on an invalid entry.");

  if (entry->scheduler() != this)
    throw torrent::internal_error("Scheduler::erase(...) called on an entry that is in another scheduler.");

  auto itr = std::find_if(begin(), end(), [entry](const SchedulerEntry* e) {
      return e == entry;
    });

  if (itr == end())
    throw torrent::internal_error("Scheduler::erase(...) could not find item in queue.");

  entry->set_scheduler(nullptr);
  entry->set_time(Scheduler::time_type{});

  auto pos = static_cast<size_t>(std::distance(begin(), itr));
  auto sz  = size();

  if (pos + 1 < sz) {
    base_type::operator[](pos) = std::move(base_type::operator[](sz - 1));
    base_type::pop_back();
    sift_up(begin(), pos);
    sift_down(begin(), size(), pos);
  } else {
    base_type::pop_back();
  }
}

void
Scheduler::wait_until(SchedulerEntry* entry, Scheduler::time_type time) {
  assert(m_thread_id == std::thread::id() || m_thread_id == std::this_thread::get_id());

  if (time == Scheduler::time_type())
    throw torrent::internal_error("Scheduler::wait_until(...) received a bad timer.");

  if (time < Scheduler::time_type(365 * 24h))
    throw torrent::internal_error("Scheduler::wait_until(...) received a too small timer.");

  if (!entry->is_valid())
    throw torrent::internal_error("Scheduler::wait_until(...) called on an invalid entry.");

  if (entry->is_scheduled())
    throw torrent::internal_error("Scheduler::wait_until(...) called on an already scheduled entry.");

  entry->set_scheduler(this);
  entry->set_time(time);

  base_type::push_back(entry);
  push_heap();
}

void
Scheduler::wait_for(SchedulerEntry* entry, Scheduler::time_type time) {
  if (time > Scheduler::time_type(10 * 365 * 24h))
    throw torrent::internal_error("Scheduler::wait_after(...) received a too large timer.");

  wait_until(entry, m_cached_time + time);
}

void
Scheduler::wait_for_ceil_seconds(SchedulerEntry* entry, Scheduler::time_type time) {
  if (time > Scheduler::time_type(10 * 365 * 24h))
    throw torrent::internal_error("Scheduler::wait_after_ceil_seconds(...) received a too large timer.");

  wait_until(entry, ceil_seconds(m_cached_time + time));
}

void
Scheduler::update_wait_until(SchedulerEntry* entry, Scheduler::time_type time) {
  assert(m_thread_id == std::thread::id() || m_thread_id == std::this_thread::get_id());

  if (time == Scheduler::time_type())
    throw torrent::internal_error("Scheduler::update_wait(...) received a bad timer.");

  if (time < Scheduler::time_type(365 * 24h))
    throw torrent::internal_error("Scheduler::update_wait(...) received a too small timer.");

  if (!entry->is_valid())
    throw torrent::internal_error("Scheduler::update_wait(...) called on an invalid entry.");

  if (entry->is_scheduled()) {
    if (entry->scheduler() != this)
      throw torrent::internal_error("Scheduler::update_wait(...) called on an entry that is in another scheduler.");

    auto old_time = entry->time();

    entry->set_time(time);

    // Find position and sift instead of O(N) make_heap()
    auto itr = std::find(begin(), end(), entry);
    if (itr == end()) {
      // Entry was removed from heap (e.g. by reentrant call), treat as new.
      entry->set_scheduler(this);
      entry->set_time(time);
      base_type::push_back(entry);
      push_heap();
      return;
    }
    auto pos = static_cast<size_t>(std::distance(begin(), itr));

    if (time < old_time)
      sift_up(begin(), pos);
    else if (time > old_time)
      sift_down(begin(), size(), pos);

    return;
  }

  entry->set_scheduler(this);
  entry->set_time(time);

  base_type::push_back(entry);
  push_heap();
}

void
Scheduler::update_wait_for(SchedulerEntry* entry, Scheduler::time_type time) {
  if (time > Scheduler::time_type(10 * 365 * 24h))
    throw torrent::internal_error("Scheduler::update_wait_after(...) received a too large timer.");

  update_wait_until(entry, m_cached_time + time);
}

void
Scheduler::update_wait_for_ceil_seconds(SchedulerEntry* entry, Scheduler::time_type time) {
  if (time > Scheduler::time_type(10 * 365 * 24h))
    throw torrent::internal_error("Scheduler::update_wait_after_ceil_seconds(...) received a too large timer.");

  update_wait_until(entry, ceil_seconds(m_cached_time + time));
}

void
Scheduler::perform(Scheduler::time_type current_time) {
  fprintf(stderr, "%lld [sched] perform size=%zu cur=%lld\n",
          (long long)time_since_epoch().count(), size(), (long long)current_time.count()); fflush(stderr);
  while (!empty() && base_type::operator[](0)->time() <= current_time) {
    auto entry = base_type::operator[](0);
    assert(entry != nullptr);

    if (size() > 1) {
      base_type::operator[](0) = std::move(base_type::operator[](size() - 1));
      base_type::pop_back();
      sift_down(begin(), size(), 0);
    } else {
      base_type::pop_back();
    }

    entry->set_scheduler(nullptr);
    entry->set_time(Scheduler::time_type{});
    entry->slot()();
  }
}

} // namespace torrent::utils
