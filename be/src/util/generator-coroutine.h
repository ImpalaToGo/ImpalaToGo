/*
 * @file generator-coroutine.h
 * @brief Due to similar nature of generators and coroutines and to avoid using third-party
 * complicated mechanisms to get coroutines into c++ code, there's macro implementation of
 * generator/continuation which may be used in place where coroutine is needed, thus, near
 * "emit", "yield return" .NET / Python style
 *
 * For original idea, see  http://www.chiark.greenend.org.uk/~sgtatham/coroutines.html
 *
 * Sample of usage.
 * 1. Define custom generator:
 *
 * $generator(my_gen){
   // All variables used in the generator should be placed here
   int i; // the counter
   int min_value;
   int max_value;

   // constructor of the generator
   my_gen(int min, int max) { min_value = min; max_value = max;}

   // starting from $emit to $stop there's the body of this generator:

   $emit(int) // declare that we are going to emit int values.
   // Should start the body of the generator.
   for (i = max_value; i > min_value; --i)
         $yield(i); // yield return in C#, yield in Python.
                    // Here, returns next number in [min_value..max_value], reversed.

   $stop; // stop, end of sequence.
   // End of body of the generator.
};

 * 2. Now the generator may be used as below:
 * // create generator:
 * my_gen gen(1, 10);
   for(int n; gen(n);) // "get next" generator invocation
      printf("next number is %d\n", n);
   return 0;
 *
 * Comment: The gen(n) is an invocation of the bool operator()(int& val) underlying method of the generator object.
 * It returns true if the parameter val was set, and false if the generator cannot provide more elements (stopped, reached $stop).
 * Here, for(int n; gen(n);) ~ for(var n in gen).
 *
 * @date   Nov 11, 2014
 * @author elenav
 */

#ifndef GENERATOR_COROUTINE_H_
#define GENERATOR_COROUTINE_H_

struct _generator
{
  int _line;
  _generator():_line(0) {}
};

#define $generator(NAME) struct NAME : public _generator

#define $emit(T) bool operator()(T& _rv) { \
                    switch(_line) { case 0:;

#define $stop  } _line = 0; return false; }

#define $yield(V)     \
        do {\
            _line=__LINE__;\
            _rv = (V); return true; case __LINE__:;\
        } while (0)


#endif /* GENERATOR_COROUTINE_H_ */
