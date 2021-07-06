#pragma once
#include <unordered_map>
#include <deque>
#include <atomic>
#include <future>
#include <mutex>
#include <condition_variable>
#include <cassert>
#include "IConsumer.h"

//----------------------------------------------------------------------------------------------------------
// class MqpS
//----------------------------------------------------------------------------------------------------------
template<typename Key, typename Value, size_t max_capacity = 1000>
class MqpS
{
// types
// data
private:
	std::atomic<bool>								running_{false};
	std::future<void>								fut_;
	//
	std::mutex										queue_mtx_;
	std::condition_variable							queue_cv_;
	std::deque<std::pair<Key, Value>>				queue_;
	//
	std::mutex										cons_mtx_;
	std::unordered_map<Key, IConsumer<Key, Value>*>	consumers_;
	//

	size_t											msg_processed_{0}; // temp
	size_t											msg_lost_{0}; // temp

// methods
private:
	//----------------------------------------------------------------------------------------------------------
	void _run()
	{
		std::deque<std::pair<Key, Value>> to_process;

		while (running_)
		{
			// queue_mtx_ guarded
			{
				std::unique_lock<std::mutex> lock{queue_mtx_};
				queue_cv_.wait(lock);
				// spurious wakes should be ok here - just process queue

				if (!running_)
				{
					return;
				}

				assert(to_process.empty());
				to_process.swap(queue_);
			}
			msg_processed_ += to_process.size();
			// queue_mtx_ free
			{
				std::unique_lock<std::mutex> lock{cons_mtx_};
				
				for (const auto& msg : to_process)
				{
					if (const auto cons_i{consumers_.find(msg.first)}; cons_i != consumers_.cend())
					{
						cons_i->second->Consume(msg.first, msg.second);
					}
				}
			}
			to_process.clear();
		}
	}

public:
	//----------------------------------------------------------------------------------------------------------
	~MqpS()
	{
		try
		{
			StopProcessing();
		}
		catch (const std::exception& ex)
		{
			std::cout << ex.what();
		}
		// any exception not derived from std::exception will abort the program
	}
	//----------------------------------------------------------------------------------------------------------
	void StartProcessing()
	{
		if (running_.exchange(true) == false)
		{
			fut_ = std::async(std::launch::async, [this](){ this->_run(); });
		}
	}
	//----------------------------------------------------------------------------------------------------------
	void StopProcessing()
	{
		running_ = false;
		queue_cv_.notify_one();
		if (fut_.valid())
		{
			fut_.get();
		}
	}
	//----------------------------------------------------------------------------------------------------------
	void Subscribe(const Key& id, IConsumer<Key, Value>* consumer)
	{
		std::lock_guard<std::mutex> lock{cons_mtx_};
		consumers_.try_emplace(id, consumer);
	}
	//----------------------------------------------------------------------------------------------------------
	void Unsubscribe(const Key& id)
	{
		std::lock_guard<std::mutex> lock{cons_mtx_};
		consumers_.erase(id);
	}
	//----------------------------------------------------------------------------------------------------------
	bool Enqueue(const Key& id, Value value)
	{
		std::lock_guard<std::mutex> lock{queue_mtx_};
		if (queue_.size() >= max_capacity)
		{
			// also might wait for queue-not-full-cv
			++msg_lost_;
			return false;
		}
		queue_.emplace_back(id, std::move(value));
		queue_cv_.notify_one();
		return true;
	}
	//----------------------------------------------------------------------------------------------------------
	// temp
	void dump()
	{
		std::lock_guard<std::mutex> lock{queue_mtx_};
		std::cout
			<< "msg enqueued: " << queue_.size()
			<< "\nmsg processed: " << msg_processed_
			<< "\nmsg lost: " << msg_lost_
			<< std::endl;
	};
};