#pragma once
#include "Mqp.hpp"

//----------------------------------------------------------------------------------------------------------
// class IConsumer
//----------------------------------------------------------------------------------------------------------
template<typename Key, typename Value>
class Consumer
	: public IConsumer<Key, Value>
{
public:
	void Consume([[maybe_unused]] Key id, [[maybe_unused]] const Value& value) override
	{
//		std::this_thread::sleep_for(1ns);
	}
};
