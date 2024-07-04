#pragma once

#ifdef _MSC_VER
	#ifdef _USRDLL	
		#define EXPORT_DECL _declspec(dllexport)
	#else
		#define EXPORT_DECL __declspec(dllimport)
	#endif
	#define HIDEVISIBILITY 
#else
	#define EXPORT_DECL
	#ifndef MAC
	#define HIDEVISIBILITY __attribute__ ((visibility("hidden")))
	#else
	#define HIDEVISIBILITY
	#endif
#endif