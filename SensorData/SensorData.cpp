#define UNICODE
#define _UNICODE

#define _CRT_SECURE_NO_WARNINGS
#define _CRT_NON_CONFORMING_SWPRINTFS

#include <windows.h>
#include <tchar.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <io.h>


DWORD WINAPI sensorThreads(LPVOID);
FLOAT findMax(FLOAT *, DWORD);
FLOAT findMin(FLOAT*, DWORD);

/*global variables and data structures*/
DWORD M, NUM_RECORDS, i;
CRITICAL_SECTION cs;
TCHAR inputFileName[30];

typedef struct threads {
	TCHAR inputFile[30];
	TCHAR outputFile[30];
}threads_t;

typedef struct sensors {
	TCHAR sensorName[30];
	FLOAT* data;
	DWORD totalData;
	FLOAT average, sum, maxVal, minVal;
	HANDLE semWrite, semRead, semUpdate;
}sensors_t;

sensors_t* sensorData;

int _tmain(int argc, LPTSTR argv[]) {
	/*init*/
	_tcscpy(inputFileName, argv[1]);
	M = _ttoi(argv[2]);
	NUM_RECORDS = 0;
	i = 0;
	INT s;
	InitializeCriticalSection(&cs);

	/*get number of records*/
	HANDLE hFile;
	hFile = CreateFile(inputFileName, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	threads_t tdata; DWORD n;
	while (ReadFile(hFile, &tdata, sizeof(threads_t), &n, NULL) && n > 0) {
		NUM_RECORDS++;
	}
	_tprintf(_T("NUM RECORDS: %d\n"),NUM_RECORDS);
	CloseHandle(hFile);
	
	sensorData = (sensors_t*)malloc(NUM_RECORDS * sizeof(sensors_t));
	for (s = 0; s < NUM_RECORDS; s++) {
		sensorData[s].semRead = CreateSemaphore(NULL, 1, 1, NULL);
		sensorData[s].semWrite = CreateSemaphore(NULL, 1, 1, NULL);
		sensorData[s].semUpdate = CreateSemaphore(NULL, 1, 1, NULL);
		sensorData[s].sum = 0.0;
		sensorData[s].totalData = 0;

	}
	/*create sensor threads*/
	HANDLE* hThreads;
	hThreads = (HANDLE*)malloc(M * sizeof(HANDLE));
	DWORD j;
	DWORD* tId;
	tId = (DWORD*)malloc(M * sizeof(DWORD));
	for (j = 0; j < M; j++) {
		tId[j] = j;
		hThreads[j] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)sensorThreads, &tId[j], 0, NULL);
		Sleep(1000);
		if (hThreads[j] == INVALID_HANDLE_VALUE) {
			_tprintf(_T("error creating threads\n"));
			return 1;
		}
	}
	WaitForMultipleObjects(M, hThreads, TRUE, INFINITE);
	for (j = 0; j < M; j++) {
		CloseHandle(hThreads[j]);
	}
	/*check output files*/
	_tprintf(_T("CHECKING OUTPUT FILES\n"));
	_tprintf(_T("=================================================================================\n"));
	
	HANDLE h1,h2,h3;

	h1 = CreateFile(_T("output001.dat"), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	h2 = CreateFile(_T("output002.dat"), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	h3 = CreateFile(_T("output003.dat"), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	sensors_t sData; DWORD nOut;

	_tprintf(_T("****************output001.dat************************\n"));
	while (ReadFile(h1, &sData, sizeof(sensors_t), &nOut, NULL) && nOut > 0) {
		_tprintf(_T("sensorName: %s\n"),sData.sensorName);
		_tprintf(_T("total Data: %d\n"), sData.totalData);
		_tprintf(_T("Average: %f\n"), sData.average);
		_tprintf(_T("Sum: %f\n"), sData.sum);
		_tprintf(_T("Max Val: %f\n"), sData.maxVal);
		_tprintf(_T("Min Val: %f\n"), sData.minVal);
		_tprintf(_T("\n"));
	}
	CloseHandle(h1);
	_tprintf(_T("****************output002.dat************************\n"));
	while (ReadFile(h2, &sData, sizeof(sensors_t), &nOut, NULL) && nOut > 0) {
		_tprintf(_T("sensorName: %s\n"), sData.sensorName);
		_tprintf(_T("total Data: %d\n"), sData.totalData);
		_tprintf(_T("Average: %f\n"), sData.average);
		_tprintf(_T("Sum: %f\n"), sData.sum);
		_tprintf(_T("Max Val: %f\n"), sData.maxVal);
		_tprintf(_T("Min Val: %f\n"), sData.minVal);
		_tprintf(_T("\n"));
	}
	CloseHandle(h2);
	_tprintf(_T("****************output003.dat************************\n"));
	while (ReadFile(h3, &sData, sizeof(sensors_t), &nOut, NULL) && nOut > 0) {
		_tprintf(_T("sensorName: %s\n"), sData.sensorName);
		_tprintf(_T("total Data: %d\n"), sData.totalData);
		_tprintf(_T("Average: %f\n"), sData.average);
		_tprintf(_T("Sum: %f\n"), sData.sum);
		_tprintf(_T("Max Val: %f\n"), sData.maxVal);
		_tprintf(_T("Min Val: %f\n"), sData.minVal);
		_tprintf(_T("\n"));
	}
	CloseHandle(h3);
	_tprintf(_T("****************************************\n"));
	


}
DWORD WINAPI sensorThreads(LPVOID lpParam) {

	DWORD* id = (DWORD*)lpParam;
	//_tprintf(_T("thread %d\n"), *id);
	
	HANDLE hFile, hIn, hOut, hFileNew;
	threads_t tData; DWORD n;
	
	DWORD nR, nW, totData;
	FLOAT sData;

	OVERLAPPED ov = { 0,0,0,0,NULL };
	LARGE_INTEGER filePos, fileReserved;
	
	hFile = CreateFile(inputFileName, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	while (i < NUM_RECORDS) {
		Sleep(3000);
		if (i >= NUM_RECORDS)
			break;
		WaitForSingleObject(sensorData[i].semUpdate, INFINITE);
		fileReserved.QuadPart = 1 * sizeof(threads_t);
		filePos.QuadPart = i * sizeof(threads_t);
		ov.Offset = filePos.LowPart;
		ov.OffsetHigh = filePos.HighPart;
		ov.hEvent = 0;
		_tprintf(_T("THREAD %d WILL READ POSITION %d\n"), *id, i);
		ReleaseSemaphore(sensorData[i].semUpdate, 1, NULL);



		LockFileEx(hFile, LOCKFILE_EXCLUSIVE_LOCK, 0, fileReserved.LowPart, fileReserved.HighPart, &ov);
				if (ReadFile(hFile, &tData, sizeof(threads_t), &n, &ov))
					_tprintf(_T("thread %d is reading with lock: %s %s\n"), *id, tData.inputFile, tData.outputFile);
				hIn = CreateFile(tData.inputFile, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
				

				/*read the inputFile in ME*/
				WaitForSingleObject(sensorData[i].semRead, INFINITE);

				while (ReadFile(hIn, &sData, sizeof(FLOAT), &nR, NULL) && nR > 0) {
					sensorData[i].totalData++;
					sensorData[i].sum += sData;
				}
				sensorData[i].data = (FLOAT*)malloc(sensorData[i].totalData * sizeof(FLOAT));
				CloseHandle(hIn);
				/*reopen the file*/
				INT j = 0;
				hFileNew = CreateFile(tData.inputFile, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
				while (ReadFile(hFileNew, &sData, sizeof(FLOAT), &nR, NULL) && nR > 0) {
						sensorData[i].data[j] = sData;
					j++;
				}
				_tprintf(_T("thread %d read data from: %s: "), *id, tData.inputFile);
				int s;
				for (s = 0; s < sensorData[i].totalData; s++) {
					_tprintf(_T("%f "), sensorData[i].data[s]);
				}
				_tcscpy(sensorData[i].sensorName, tData.inputFile);
				sensorData[i].maxVal = findMax(sensorData[i].data, sensorData[i].totalData);
				sensorData[i].minVal = findMin(sensorData[i].data, sensorData[i].totalData);
				sensorData[i].average = (sensorData[i].sum / sensorData[i].totalData);
			ReleaseSemaphore(sensorData[i].semRead, 1, NULL);

			/*write to outputFile*/
			_tprintf(_T("thread %d writing to file: %s\n"),*id, tData.outputFile);
			WaitForSingleObject(sensorData[i].semWrite, INFINITE);
				hOut = CreateFile(tData.outputFile, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
				DWORD writePos=0; sensors_t se; DWORD nOut;
				while (ReadFile(hOut, &se, sizeof(sensors_t), &nOut, NULL) && nOut > 0) {
					writePos++;
				}
				OVERLAPPED ovW = { 0,0,0,0,NULL };
				LARGE_INTEGER pos;
				pos.QuadPart = (writePos+1) * sizeof(sensors_t);
				ovW.Offset = pos.LowPart;
				ovW.OffsetHigh = pos.HighPart;
				ovW.hEvent = 0;

				WriteFile(hOut, &sensorData[i], sizeof(sensors_t), &nW, &ovW);
				_tprintf(_T("thread %d wrote output to %s\n"), *id, tData.outputFile);
				CloseHandle(hOut);
			ReleaseSemaphore(sensorData[i].semWrite,1,NULL);

		UnlockFileEx(hFile, 0, fileReserved.LowPart, fileReserved.HighPart, &ov);
		
		WaitForSingleObject(sensorData[i].semUpdate, INFINITE);
			i++;
		ReleaseSemaphore(sensorData[i].semUpdate,1,NULL);
		//CloseHandle(hOut);
		CloseHandle(hFileNew);
	}

	CloseHandle(hFile);

	return 0;
}


FLOAT findMax(FLOAT* arr, DWORD size) {
	INT i;
	FLOAT max;
	max = arr[0];
	for (i = 0; i < size; i++) {
		if (arr[i] > max)
			max = arr[i];
	}
	return max;
}

FLOAT findMin(FLOAT* arr, DWORD size) {
	INT i;
	FLOAT min;
	min = arr[0];
	for (i = 0; i < size; i++) {
		if (arr[i] < min)
			min = arr[i];
	}
	return min;
}