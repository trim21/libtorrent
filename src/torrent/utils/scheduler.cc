#include "config.h"

#include "torrent/utils/scheduler.h"

#include <algorithm>
#include <cassert>
#include <cstddef>

#include "torrent/exceptions.h"
#include "torrent/utils/chrono.h"

namespace torrent::utils {

static constexpr auto compare = [](const SchedulerEntry* a, const SchedulerEntry* b) {
  return a->time() > b->time();
};

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

void
Scheduler::erase(SchedulerEntry* entry) {
  assert(m_thread_id == std::thread::id() || m_thread_id == std::this_thread::get_id());

  if (!entry->is_scheduled())
    return;

  if (!entry->is_valid())
    throw torrent::internal_error("Scheduler::erase(...) called on an invalid entry.");

  if (entry->scheduler() != this)
    throw torrent::internal_error("Scheduler::erase(...) called on an entry that is in another scheduler.");

  // Lazy deletion: mark as dead by clearing scheduler pointer and setting
  // time to zero. The entry stays in the heap and will be skipped by perform().
  entry->set_scheduler(nullptr);
  entry->set_time(Scheduler::time_type{});
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

    // Lazy update: mark old entry as dead (time zero, scheduler null).
    // The old entry stays in the heap and will be skipped by perform().
    entry->set_scheduler(nullptr);
    entry->set_time(Scheduler::time_type{});
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
  while (!empty() && base_type::operator[](0)->time() <= current_time) {
    auto entry = base_type::operator[](0);

    std::pop_heap(begin(), end(), compare);
    base_type::pop_back();

    if (!entry->is_scheduled()) {
      // Lazy-deleted entry: scheduler was cleared by erase() or update_wait_until().
      continue;
    }

    entry->set_scheduler(nullptr);
    entry->set_time(Scheduler::time_type{});
    entry->slot()();
  }
}

} // namespace torrent::utils
