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
// class Mqp
//----------------------------------------------------------------------------------------------------------
template<typename Key, typename Value, size_t max_capacity = 1000>
class Mqp
{
// types
private:
	//----------------------------------------------------------------------------------------------------------
	// struct ConsumerQueue
	//----------------------------------------------------------------------------------------------------------
	struct ConsumerQueue
	{
		IConsumer<Key, Value>*	consumer_{nullptr};
		std::list<Value>		queue_;

		ConsumerQueue() = default;
		ConsumerQueue(IConsumer<Key, Value>* consumer)
			: consumer_{consumer}
		{}
		void process_queue(const Key& id)
		{
			assert(consumer_);
			for (const Value& val : queue_)
			{
				consumer_->Consume(id, val);
			}
			queue_.clear();
		}
	};

// data
private:
	std::unordered_map<Key, ConsumerQueue>	queues_;
	std::atomic<bool>		running_{false};
	std::mutex				mtx_;
	std::condition_variable cv_;
	std::future<void>		fut_;

	size_t					msg_processed_{0}; // temp
	size_t					msg_lost_{0}; // temp
// methods
private:
	//----------------------------------------------------------------------------------------------------------
	void _run()
	{
		while (running_)
		{
			std::unique_lock<std::mutex> lock{mtx_};
			cv_.wait(lock);
			// spurious wakes should be ok here - just process queues
			if (!running_)
			{
				return;
			}

			for (auto& q_pr : queues_)
			{
				if (q_pr.second.consumer_)
				{
					msg_processed_ += q_pr.second.queue_.size();
					q_pr.second.process_queue(q_pr.first);
				}
			}
		}
	}

public:
	//----------------------------------------------------------------------------------------------------------
	~Mqp()
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
		cv_.notify_one();
		if (fut_.valid())
		{
			fut_.get();
		}
	}
	//----------------------------------------------------------------------------------------------------------
	void Subscribe(const Key& id, IConsumer<Key, Value>* consumer)
	{
		std::lock_guard<std::mutex> lock{mtx_};
		ConsumerQueue& cq{queues_[id]};
		if (!cq.consumer_)
		{
			cq.consumer_ = consumer;
			cv_.notify_one();
		}
	}
	//----------------------------------------------------------------------------------------------------------
	void Unsubscribe(const Key& id)
	{
		std::lock_guard<std::mutex> lock{mtx_};
		queues_.erase(id);
	}
	//----------------------------------------------------------------------------------------------------------
	bool Enqueue(const Key& id, Value value)
	{
		std::lock_guard<std::mutex> lock{mtx_};
		ConsumerQueue& cq{queues_[id]};
		if (cq.queue_.size() >= max_capacity)
		{
			// also might wait for queue-not-full-cv
			++msg_lost_;
			return false;
		}
		cq.queue_.emplace_back(std::move(value));
		cv_.notify_one();
		return true;
	}
	//----------------------------------------------------------------------------------------------------------
	// temp
	void dump()
	{
		std::lock_guard<std::mutex> lock{mtx_};
		std::cout << "id\tqueue size\n";
		for (auto& q_pr : queues_)
		{
			if (!q_pr.second.queue_.empty())
			{
				std::cout << q_pr.first << '\t' << q_pr.second.queue_.size() << '\n';
			}
		}
		std::cout
			<< "msg processed: " << msg_processed_ << std::endl
			<< "msg lost: " << msg_lost_ << std::endl;
	};
};