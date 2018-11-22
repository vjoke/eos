/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */

#pragma once

#include <thread>
#include <atomic>
#include <vector>
#include <boost/noncopyable.hpp>
#include <iostream>

#include "consumer_core.h"
#include "fifo.h"

namespace eosio {

template<typename T>
class consumer final : public boost::noncopyable
{
public:
    consumer(std::unique_ptr<consumer_core<T>> core);
    ~consumer();

    uint32_t push(const T& element);

private:
    void run();

    fifo<T> m_fifo;
    std::unique_ptr<consumer_core<T>> m_core;
    std::atomic<bool> m_exit;
    std::unique_ptr<std::thread> m_thread;
};

template<typename T>
consumer<T>::consumer(std::unique_ptr<consumer_core<T> > core):
    m_fifo(fifo<T>::behavior::blocking),
    m_core(std::move(core)),
    m_exit(false),
    m_thread(std::make_unique<std::thread>([&]{this->run();}))
{

}

template<typename T>
consumer<T>::~consumer()
{
    m_fifo.set_behavior(fifo<T>::behavior::not_blocking);
    m_exit = true;
    m_thread->join();
    m_core = NULL;
    std::cout << ">>>> consumer destroyed" << "\n";
}

template<typename T>
uint32_t consumer<T>::push(const T& element)
{
    uint32_t len = m_fifo.push(element);
    return len;
}

template<typename T>
void consumer<T>::run()
{
    std::cout << ">>>> consumer thread start" << "\n";
    while (!m_exit)
    {
        auto elements = m_fifo.pop_all();
        m_core->consume(elements);
    }
    // Pop remaining messages if any
    auto elements = m_fifo.pop_all();
    if (elements.size() > 0) {
        std::cout << ">>>> remaining " << elements.size() << " messages on exit" << "\n";
        m_core->consume(elements);
    }
    
    std::cout << ">>>> consumer thread ended" << "\n";
}

} // namespace

