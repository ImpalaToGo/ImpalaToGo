/*
 * synchronization-utils.hpp
 *
 *  Created on: Oct 1, 2014
 *      Author: elenav
 */

#ifndef SYNCHRONIZATION_UTILS_HPP_
#define SYNCHRONIZATION_UTILS_HPP_

#include <boost/thread.hpp>

/**
 * @namespace impala
 */
namespace impala{

namespace synchronization{

class Event;
   typedef Event* Event_handle;
   static const unsigned K_INFINITE = 0xFFFFFFFF;

   class Event
   {
      friend Event_handle CreateEvent( void );
      friend void CloseHandle( Event_handle evt );
      friend void SetEvent( Event_handle evt );
      friend void WaitForSingleObject( Event_handle evt, unsigned timeout );

      Event( void ) : m_bool(false) { }

      bool m_bool;
      boost::mutex m_mutex;
      boost::conditional m_condition;
   };

   Event_handle CreateEvent( void )
   { return new Event; }

   void CloseHandle( Event_handle evt )
   { delete evt; }

   void SetEvent( Event_handle evt )
   {
      evt->m_bool = true;
      evt->m_condition.notify_all();
   }

   void WaitForSingleObject( Event_handle evt, unsigned timeout )
   {
      boost::scoped_static_mutex_lock lock( evt->m_mutex );
      if( timeout == K_INFINITE )
      {
         while( !evt->m_bool )
         {
            evt->m_condition.wait( lock );
         }
      }
      else
      {
         //slightly more complex code for timeouts
      }
   }
}

}

#endif /* SYNCHRONIZATION_UTILS_HPP_ */
