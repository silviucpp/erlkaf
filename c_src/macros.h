#ifndef C_SRC_MACROS_H_
#define C_SRC_MACROS_H_

#define UNUSED(expr) do { (void)(expr); } while (0)

#define DISALLOW_ASSIGN(TypeName) void operator=(const TypeName&)
#define DISALLOW_COPY_AND_ASSIGN(TypeName) TypeName(const TypeName&); DISALLOW_ASSIGN(TypeName)
#define DISALLOW_IMPLICIT_CONSTRUCTORS(TypeName) TypeName(); DISALLOW_COPY_AND_ASSIGN(TypeName)

#define scoped_ptr(Name, Type, New, Free) std::unique_ptr<Type, decltype(&Free)>Name (New, &Free)

#ifdef NDEBUG
#define ASSERT(x) UNUSED(x)
#else
#include <assert.h>
#define ASSERT(x) assert(x)
#endif

#endif  // C_SRC_MACROS_H_
