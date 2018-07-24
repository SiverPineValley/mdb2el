#define _CRT_SECURE_NO_WARNINGS
#include <python.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ini.h"

/*******************************************************************************************************************
 * 
 * 이 소스 코드는 INI config 파일을 파싱하여, MongoDB와 Elasticsearch 정보를 얻어와서 해당 정보를 Upload할 수 있게
 * 도와준다. INI 파일은 []로 Section이 구분되며, key=value와 같은 식으로 값을 가져야 한다. 파일 명은 꼭 mdb2el.ini이어야 한다.
 *
 * 이 소스코드를 실행하기 위해서는, ini.c, ini.h, mdb2el,c, mdb2el.ini, mdb2el.py를 같은 폴대 내에 위치시켜야 하며,
 * Python은 3.6 버젼을 사용했다. 추가적으로 pymongo, elasticsearch, tqdm 모듈이 설치되어 있어야 한다.
 *
 * 컴파일을 하기 위해서는 다음과 같은 Command를 입력한다.
 * # Windows
 * gcc -o mdb2el mdb2el.c ini.c -L "C:\Users\UserName\Anaconda3\libs" -I "C:\Users\UserName\Anaconda3\include" -lpython36
 *
 * # Linux
 * gcc -o mdb2el mdb2el.c ini.h ini.c $(python3-config --libs --includes)
 * 
 * 이후, ./mdb2el로 실행시킬 수 있다.
 * 
 *********************************************************************************************************************/

typedef struct{
	char* dbname;
	char* colname;
	char* elidx;
	char* eltype;
} info;

char* prevSection = "";
int numofSections = 0;
info *iniInfo;

void mdb2el( char* dbname, char* colname, char* elidx, char* eltype ) {
	// 모듈을 불러온다.
	PyObject *mymdb2el = PyImport_ImportModule("mdb2el");
	
	if(mymdb2el == NULL) {
		printf("The Module is not correctly imported \n");
		return;
	}

	// 최초 변수 선언
	PyObject *dname = PyUnicode_FromString(dbname);
	PyObject *cname = PyUnicode_FromString(colname);
	PyObject *eidx = PyUnicode_FromString(elidx);
	PyObject *etype = PyUnicode_FromString(eltype);
	
	// Argument로 사용할 수 있도록 Tuple로 묶어 준다.
	PyObject *args = PyTuple_Pack(4, dname, cname, eidx, etype);
	if(args == NULL) {
		printf("The variables are not correctly packed \n");
		Py_XDECREF(dname);
		Py_XDECREF(cname);
		Py_XDECREF(eidx);
		Py_XDECREF(etype);
		return;
	}
	

	// mdb2el Module에서 mdb2el Class를 불러오고, 객체를 생성한다.
	PyObject *myClass = PyObject_GetAttrString(mymdb2el, "mdb2el");
	if(myClass != NULL) {
		Py_XDECREF(mymdb2el);
		PyObject *myObject = PyObject_CallObject(myClass, args);
		if(myObject != NULL) {
			Py_XDECREF(myClass);
			// // run 함수를 사용한다.
			PyObject_CallMethod(myObject, "run", NULL);
		}
		else {
			Py_XDECREF(myClass);
			Py_XDECREF(myObject);
			return;
		}
		Py_XDECREF(myObject);
	}
	else {
		Py_XDECREF(mymdb2el);
		return;
	}
	
}

static int handler(void* configs, const char* section, const char* key, const char* value)
{	
	// 새로운 Section이 나타나면, Heap Memory 영역을 확장한다.
    if(strcmp(prevSection, section) != 0) {
	 	prevSection = strdup(section);
    	numofSections++;
    	if(numofSections > 1) {
    		if ( (iniInfo = (info*) realloc( iniInfo, numofSections*sizeof(info) ) ));
    		else {
    			// 메모리 영역이 Storage 범위를 넘어서면 종료한다.
    			printf("Out of Storage");
    			return 0;
    		}
    	}
    	
    }

    // 구조체의 각 종류 별로 value 값을 저장한다.
    if (strcmp(key, "dbname") == 0) {
        iniInfo[numofSections-1].dbname = strdup(value);
    } else if (strcmp(key, "colname") == 0) {
        iniInfo[numofSections-1].colname = strdup(value);
    } else if (strcmp(key, "elidx") == 0) {
        iniInfo[numofSections-1].elidx = strdup(value);
    } else if (strcmp(key, "eltype") == 0) {
    	iniInfo[numofSections-1].eltype = strdup(value);
    } else {
    	// 올바른 key, value가 아니라면, 종료한다.
        return 0;  
    }
    return 1;
}

void send(void) {
	for(int i = 0; i < numofSections; i++) mdb2el(iniInfo[i].dbname, iniInfo[i].colname, iniInfo[i].elidx, iniInfo[i].eltype);
}

void parse_config(info* iniInfos) {

	if (ini_parse("mdb2el.ini", handler, iniInfos) < 0) {
		printf("Can't load 'mdb2el.ini'\n");
		return;
	}
}

int main(int argc, char** argv)
{

  	Py_Initialize();

	if (Py_IsInitialized()) {
	
		// Windows Module Import하는 부분
		#ifdef _WIN32
			PyRun_SimpleString(
				"import sys\n"
				"import os\n"
				"import inspect\n"

				"sys.path.append(os.path.dirname(inspect.getfile(os)))\n"
				"sys.path.append(os.path.dirname(inspect.getfile(os)) + '\\site-packages')\n"
				"sys.path.append(os.path.dirname(inspect.getfile(os)) + '\\site-packages\\win32\\lib')\n"
				"sys.path.append(os.path.dirname(inspect.getfile(os)) + '\\site-packages\\win32')\n"
				"sys.path.append(os.path.dirname(os.path.realpath('__file__')))\n"
				);

		#elif __linux__
			PyRun_SimpleString(
				"import sys\n"
				"import os\n"
				"import inspect\n"
				
				"sys.path.append(os.path.dirname(os.path.realpath('__file__')))\n"
				);
		#endif

		// 전역변수 iniInfo 구조체에 초기 메모리 영역을 지정한다.
		iniInfo = (info*) malloc(sizeof(info));
		info *dummy = (info*) malloc(sizeof(info));

		// Parsing 및 MongoDB에서 Elasticsearch까지의 전송을 수행한다.
		parse_config(dummy);
		send();

		// 할당한 Heap 메모리를 free하고 종료한다.
		free(dummy);
		free(iniInfo);
		Py_Finalize();
	}

	return 0;
}
