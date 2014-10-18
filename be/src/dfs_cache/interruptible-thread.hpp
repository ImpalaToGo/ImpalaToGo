/*
 * interruptible-thread.hpp
 *
 *  Created on: Oct 14, 2014
 *      Author: elenav
 */

#ifndef INTERRUPTIBLE_THREAD_HPP_
#define INTERRUPTIBLE_THREAD_HPP_

#include <thread>
#include <future>

/** @namespace impala */
namespace impala{

/** Utility class, just provides the management of "interrupt" flag */
class interrupt_flag{
	public:
	/** set the flag to interrupt */
	void set();

	/** getter for "flag is set" condition */
	bool is_set() const;
};

/** interrupt flag local for current thread */
thread_local interrupt_flag this_thread_interrupt_flag;

/** Represents the wrapper on std::thread with extra facility to interrupt the encapsulated thread gracefully
 * with the predefined behavior
 */
class interruptible_thread {
	std::thread internal_thread;  /**< encapsulated thread */
	interrupt_flag* flag;         /**< interrupt flag */

public:
	/**
	 * Ctor. Construct the interruptable thread with the entry point
	 */
	template<typename FunctionType>
	interruptible_thread(FunctionType f){
		std::promise<interrupt_flag*> p;
		// create thread. Set the promise to the value of "local thread interrupt flag"
		internal_thread = std::thread([f, &p]{
			p.set_value(&this_thread_interrupt_flag);
			f();
		});
		flag = p.get_future().get();
	}

	/** Interrupt the thread signal */
	void interrupt(){
		if(flag){
			flag->set();
		}
	}
};

}

#endif /* INTERRUPTIBLE_THREAD_HPP_ */
