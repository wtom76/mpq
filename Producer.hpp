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
	const Key			id_bound_;
	Queue<Key, Value>*	queue_{nullptr};
	std::atomic<bool>	running_{false};
	std::future<void>	fut_;
	const size_t		period_{1000};

// methods
private:
	//----------------------------------------------------------------------------------------------------------
	// uses default Value ctor for test purpose
	// might delay stopping for period_, but for test should be ok
	void _run()
	{
		Key id{0};
		while (running_)
		{
			for (int i = 0; i != period_; ++i)	// to wait some time
			{
				id = (id + 1) % id_bound_;
			}
			queue_->Enqueue(id, Value{});
		}
	}

public:
	//----------------------------------------------------------------------------------------------------------
	Producer(Key id_bound, size_t period)
		: id_bound_{std::move(id_bound)}
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