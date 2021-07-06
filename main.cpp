#include <string>
#include <exception>
#include <iostream>
#include <array>
#include <memory>
#include "Mqp.hpp"
#include "Producer.hpp"
#include "Consumer.hpp"

using Key = size_t;
using Value = std::string;
using MqpProducerT = Producer<Key, Value, Mqp>;
using MqpPreProducerT = Producer<Key, Value, MqpPrebuilt>;
using IConsumerT = IConsumer<Key, Value>;
using ConsumerT = Consumer<Key, Value>;

int main()
{
	constexpr size_t queue_num{64}; // number of simultaneously running queues
	constexpr auto prod_period{1us};
	constexpr size_t prod_factor{32};
	constexpr auto run_time{10s};

	try
	{
		// MqpPrebuilt test
		{
			MqpPrebuilt<Key, Value> queue;

			std::array<std::unique_ptr<IConsumerT>, queue_num> cons;
			for (size_t i{0}; i != queue_num; ++i)
			{
				cons[i] = std::make_unique<ConsumerT>();
				queue.Subscribe(i, cons[i].get());
			}

			queue.StartProcessing();

			std::array<std::unique_ptr<MqpPreProducerT>, queue_num * prod_factor> prods;
			for (size_t i{0}; i != queue_num; ++i)
			{
				for (size_t prod_i{0}; prod_i != prod_factor; ++prod_i)
				{
					prods[i * prod_factor + prod_i] = std::make_unique<MqpPreProducerT>(i, prod_period);
					prods[i * prod_factor + prod_i]->start(&queue);
				}
			}

			std::cout << "waiting...\n";
			std::this_thread::sleep_for(run_time);

			for (auto& p : prods)
			{
				p->stop();
			}

			std::cout << "stop...\n";
			queue.StopProcessing();
			queue.dump();
		}
		// Mqp test
		{
			Mqp<Key, Value> queue;

			queue.StartProcessing();

			std::array<std::unique_ptr<IConsumerT>, queue_num> cons;
			for (size_t i{0}; i != queue_num; ++i)
			{
				cons[i] = std::make_unique<ConsumerT>();
				queue.Subscribe(i, cons[i].get());
			}

			std::array<std::unique_ptr<MqpProducerT>, queue_num * prod_factor> prods;
			for (size_t i{0}; i != queue_num; ++i)
			{
				for (size_t prod_i{0}; prod_i != prod_factor; ++prod_i)
				{
					prods[i * prod_factor + prod_i] = std::make_unique<MqpProducerT>(i, prod_period);
					prods[i * prod_factor + prod_i]->start(&queue);
				}
			}

			std::cout << "waiting...\n";
			std::this_thread::sleep_for(run_time);

			for (auto& p : prods)
			{
				p->stop();
			}

			std::cout << "stop...\n";
			queue.StopProcessing();
			queue.dump();

			for (size_t i{0}; i != queue_num; ++i)
			{
				queue.Unsubscribe(i);
			}
		}
	}
	catch (const std::exception& ex)
	{
		std::cout << ex.what();
	}
}
