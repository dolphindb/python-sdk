#pragma once

#ifdef _MSC_VER
	#ifdef _USRDLL
		#define EXPORT_DECL _declspec(dllexport)
	#else
		#define EXPORT_DECL __declspec(dllimport)
	#endif
	#define HIDEVISIBILITY
#elif defined(MAC)
	#define EXPORT_DECL
	#define HIDEVISIBILITY
#else
	#define EXPORT_DECL __attribute__ ((visibility("default")))
	#define HIDEVISIBILITY __attribute__ ((visibility("hidden")))
#endif