#pragma once
#include <future>
#include <atomic>
#include <exception>
#include <iostream>
#include <chrono>
#include <cassert>
#include "Mqp.hpp"

using namespace std::literals::chrono_literals;

template<typename Key, typename Value, template <typename, typename> class Queue>
class Producer final
{
// types
// data
private:
	Key					id_;
	Queue<Key, Value>*	queue_{nullptr};
	std::atomic<bool>	running_{false};
	std::future<void>	fut_;
	std::chrono::steady_clock::duration period_{1ms};

// methods
private:
	//----------------------------------------------------------------------------------------------------------
	// uses default Value ctor for test purpose
	// might delay stopping for period_, but for test should be ok
	void _run()
	{
		while (running_)
		{
			queue_->Enqueue(id_, Value{});
			std::this_thread::sleep_for(period_);
		}
	}

public:
	//----------------------------------------------------------------------------------------------------------
	Producer(Key id, std::chrono::steady_clock::duration period)
		: id_{std::move(id)}
		, period_{period}
	{}
	//----------------------------------------------------------------------------------------------------------
	~Producer()
	{
		try
		{
			stop();
		}
		catch (const std::exception& ex)
		{
			std::cout << ex.what();
		}
	}
	//----------------------------------------------------------------------------------------------------------
	void start(Queue<Key, Value>* queue)
	{
		assert(queue);
		queue_ = queue;

		assert(!fut_.valid());
		assert(!running_);
		running_ = true;
		fut_ = std::async(std::launch::async, [this](){ this->_run(); });
	}
	//----------------------------------------------------------------------------------------------------------
	void stop()
	{
		running_ = false;
		if (fut_.valid())
		{
			fut_.get();
		}
	}
};