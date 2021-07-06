#pragma once

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
