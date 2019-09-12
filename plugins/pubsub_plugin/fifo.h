/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */

#pragma once

// #include <boost/thread/mutex.hpp>
// #include <boost/thread/condition_variable.hpp>
// #include <boost/thread/thread.hpp>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <boost/atomic.hpp>

#include <atomic>
#include <deque>
#include <vector>
#include <boost/noncopyable.hpp>


namespace eosio {

template<typename T>
class fifo : public boost::noncopyable
{
public:
    enum class behavior {blocking, not_blocking};

    fifo(behavior value);

    uint32_t push(const T& element);
    std::vector<T> pop_all();
    void set_behavior(behavior value);

private:
    std::mutex m_mux;
    std::condition_variable m_cond;
    boost::atomic<behavior> m_behavior;

    std::deque<T> m_deque;
};

template<typename T>
fifo<T>::fifo(behavior value)
{
    m_behavior = value;
}

template<typename T>
uint32_t fifo<T>::push(const T& element)
{
    if (true) {
        std::scoped_lock lock(m_mux);
        m_deque.push_back(element);
        // lock.unlock();
    }
    
    m_cond.notify_one();

    return m_deque.size();
}

template<typename T>
std::vector<T> fifo<T>::pop_all()
{
    // std::scoped_lock lock(m_mux);
    std::unique_lock<std::mutex> lock(m_mux);
    while (m_behavior == behavior::blocking && m_deque.empty()) {
        m_cond.wait(lock);
    }

    std::vector<T> result;
    while(!m_deque.empty())
    {
        result.push_back(std::move(m_deque.front()));
        m_deque.pop_front();
    }
    lock.unlock();

    return result;
}

template<typename T>
void fifo<T>::set_behavior(behavior value)
{
    m_behavior = value;
    m_cond.notify_all();
}

} // namespace


