#pragma once
#include <unordered_map>
#include <deque>
#include <atomic>
#include <future>
#include <mutex>
#include <condition_variable>
#include <cassert>

static size_t s_msg_processed{0};

//----------------------------------------------------------------------------------------------------------
// class IConsumer
//----------------------------------------------------------------------------------------------------------
template<typename Key, typename Value>
class IConsumer
{
public:
	virtual ~IConsumer(){}

	// is id really nesessary here?
	virtual void Consume(Key id, const Value& value) = 0;
};

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
		std::deque<Value>		queue_;

		ConsumerQueue()
		{}
		ConsumerQueue(IConsumer<Key, Value>* consumer)
			: consumer_{consumer}
		{}
		void process_queue(const Key& id)
		{
			if (consumer_)
			{
				for (const Value& val : queue_)
				{
					consumer_->Consume(id, val);
					++s_msg_processed;
				}
				queue_.clear();
			}
		}
	};

// data
private:
	std::unordered_map<Key, ConsumerQueue>	queues_;
	std::atomic<bool>		running_{false};
	std::mutex				mtx_;
	std::condition_variable cv_;
	std::future<void>		fut_;

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

			for (auto& q_pr : queues_)
			{
				q_pr.second.process_queue(q_pr.first);
			}
		}
	}

public:
	//----------------------------------------------------------------------------------------------------------
	Mqp()
	{
		s_msg_processed = 0;
	}
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
			return false;
		}
		cq.queue_.emplace_back(std::move(value));
		cv_.notify_one();
		return true;
	}
	//----------------------------------------------------------------------------------------------------------
	void dump()
	{
		std::lock_guard<std::mutex> lock{mtx_};
		std::cout << "id\tqueue size\n";
		for (auto& q_pr : queues_)
		{
			std::cout << q_pr.first << '\t' << q_pr.second.queue_.size() << '\n';
		}
		std::cout << "msg processed: " << s_msg_processed << std::endl;
	};
};

//----------------------------------------------------------------------------------------------------------
// class MqpPrebuilt
// difference is lock free message processing for price of having all consumers subscribed before MqpPrebuilt is started
//----------------------------------------------------------------------------------------------------------
template<typename Key, typename Value, size_t max_capacity = 1000>
class MqpPrebuilt
{
// types
private:
	//----------------------------------------------------------------------------------------------------------
	// struct ConsumerQueue
	//----------------------------------------------------------------------------------------------------------
	struct ConsumerQueue
	{
		IConsumer<Key, Value>*	consumer_{nullptr};
		std::deque<Value>		queue_;
		std::deque<Value>		internal_queue_; // lock free. processed one

		ConsumerQueue()
		{}
		ConsumerQueue(IConsumer<Key, Value>* consumer)
			: consumer_{consumer}
		{}
		void swap_queue()
		{
			assert(internal_queue_.empty());
			internal_queue_.swap(queue_);
		}
		void process_queue(const Key& id)
		{
			for (const Value& val : internal_queue_)
			{
				consumer_->Consume(id, val);
				++s_msg_processed;
			}
			internal_queue_.clear();
		}
	};

// data
private:
	std::unordered_map<Key, ConsumerQueue>	queues_;
	std::atomic<bool>		running_{false};
	std::mutex				mtx_;
	std::condition_variable cv_;
	std::future<void>		fut_;

// methods
private:
	//----------------------------------------------------------------------------------------------------------
	void _run()
	{
		while (running_)
		{
			{
				std::unique_lock<std::mutex> lock{mtx_};
				cv_.wait(lock);
				// spurious wakes should be ok here - just process queues

				for (auto& q_pr : queues_)
				{
					q_pr.second.swap_queue();
				}
			}
			for (auto& q_pr : queues_)
			{
				q_pr.second.process_queue(q_pr.first);
			}
		}
	}

public:
	//----------------------------------------------------------------------------------------------------------
	MqpPrebuilt()
	{
		s_msg_processed = 0;
	}
	//----------------------------------------------------------------------------------------------------------
	~MqpPrebuilt()
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
		assert(!running_);
		ConsumerQueue& cq{queues_[id]};
		if (!cq.consumer_)
		{
			cq.consumer_ = consumer;
			cv_.notify_one();
		}
	}
	//----------------------------------------------------------------------------------------------------------
	bool Enqueue(const Key& id, Value value)
	{
		std::lock_guard<std::mutex> lock{mtx_};
		ConsumerQueue& cq{queues_[id]};
		if (cq.queue_.size() >= max_capacity)
		{
			// also might wait for queue-not-full-cv
			return false;
		}
		cq.queue_.emplace_back(std::move(value));
		cv_.notify_one();
		return true;
	}
	//----------------------------------------------------------------------------------------------------------
	void dump()
	{
		std::lock_guard<std::mutex> lock{mtx_};
		std::cout << "id\tqueue size\tinternal_queue size\n";
		for (auto& q_pr : queues_)
		{
			std::cout << q_pr.first << '\t' << q_pr.second.queue_.size() << '\t' << q_pr.second.internal_queue_.size() << '\n';
		}
		std::cout << "msg processed: " << s_msg_processed << std::endl;
	};
};