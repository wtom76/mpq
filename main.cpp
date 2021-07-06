#include <string>
#include <exception>
#include <iostream>
#include <array>
#include <memory>
#include "Mqp.hpp"
#include "MqpS.hpp"
#include "Producer.hpp"
#include "Consumer.hpp"

using Key = size_t;
using Value = std::string;

template <template <typename, typename> class QueueT>
void test()
{
	using MqpProducerT = Producer<Key, Value, QueueT>;
	using IConsumerT = IConsumer<Key, Value>;
	using ConsumerT = Consumer<Key, Value>;

	constexpr size_t queue_num{1'000'000};	// number of consumers
	constexpr size_t prod_num{15};			// number of producers
	constexpr size_t prod_period{1000};		// message creation delay ...ish
	constexpr auto run_time{10s};			// test run time

	try
	{
		// Mqp test
		{
			std::unique_ptr<QueueT<Key, Value>> queue{std::make_unique<QueueT<Key, Value>>()};

			queue->StartProcessing();

			std::vector<std::unique_ptr<IConsumerT>> cons(queue_num);
			for (size_t i{0}; i != queue_num; ++i)
			{
				cons[i] = std::make_unique<ConsumerT>();
				queue->Subscribe(i, cons[i].get());
			}

			std::vector<std::unique_ptr<MqpProducerT>> prods(prod_num);
			for (size_t i{0}; i != prod_num; ++i)
			{
				prods[i] = std::make_unique<MqpProducerT>(queue_num, prod_period);
				prods[i]->start(queue.get());
			}

			std::cout << "running...\n";
			std::this_thread::sleep_for(run_time);

			for (auto& p : prods)
			{
				p->stop();
			}

			std::cout << "stop...\n";
			queue->StopProcessing();
			queue->dump();

			for (size_t i{0}; i != queue_num; ++i)
			{
				queue->Unsubscribe(i);
			}
		}
	}
	catch (const std::exception& ex)
	{
		std::cout << ex.what();
	}
}

int main()
{
	std::cout << "MqpS\n";
	test<MqpS>();
	std::cout << "Mqp\n";
	test<Mqp>();
}
