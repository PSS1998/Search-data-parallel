#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h> 
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h> 
#include <iostream>
#include <sstream>
#include <algorithm>
#include <iterator>

using namespace std;


char* itoa(int val, int base){
	
	static char buf[32] = {0};
	
	int i = 30;
	
	for(; val && i ; --i, val /= base)
	
		buf[i] = "0123456789abcdef"[val % base];
	
	return &buf[i+1];
	
}

void print_query_reverse(char query_result[1000][10][20], int query_result_num, int num_columns){
	for(int i=1; i<=query_result_num; i++){
		for(int j=0; j<=num_columns; j++){
			printf("%s ", query_result[query_result_num-i][j]);
		}
		printf("\n");
	}
}

void print_query(char query_result[1000][10][20], int query_result_num, int num_columns){
	for(int i=0; i<=query_result_num-1; i++){
		for(int j=0; j<=num_columns; j++){
			printf("%s ", query_result[i][j]);
		}
		printf("\n");
	}
}


void insertionSort(char arr[1000][10][20], int n, int sorting_index, int num_columns) { 
   int i, j;
   char key[20]; 
   for (i = 1; i < n; i++)
   { 
       strcpy(key, arr[i][sorting_index]);
       char temp[10][20]; 
       for(int w=0; w<num_columns; w++){
    		strcpy(temp[w], arr[i][w]); 
       }
       j = i-1; 
  
       while (j >= 0 && strcmp(arr[j][sorting_index], key)>0) 
       { 
       		for(int w=0; w<num_columns; w++){
        		strcpy(arr[j+1][w], arr[j][w]); 
        	}
        	j = j-1; 
       }
       for(int w=0; w<num_columns; w++){
    		strcpy(arr[j+1][w], temp[w]); 
       }
   } 
}


int main(int argc, char **argv){
	
	int n;
	int fd1;
	char myfifo[20];
	int sorting_index; 
	char sort_value[20]; 
	int isSorted = 0;  
	int num_columns;

	char query_result[1000][10][20];

	int query_result_num = 0;

	
	if(argc > 3){
		isSorted = 1;
		sorting_index = atoi(argv[2]);
		strcpy(sort_value, argv[3]);
	}

  
    char str1[800], str2[800]; 
    int num_workers = 0;
    int workers_done = 0;
    if(isSorted){
		num_workers = atoi(argv[4]);
    }
    else{
    	num_workers = atoi(argv[2]);
    }

	strcpy(myfifo, argv[1]);

    
    while (1) 
    { 



    	fd_set fds;
		int maxfd;
		int res;
		char buf[256];


       	char myfifo_temp[40];
		strcpy(myfifo_temp, myfifo);
		strcat(myfifo_temp, to_string(workers_done).c_str());
		
       	int namedpipe_fd = open(myfifo_temp, O_RDONLY);
       	int n = 0;
       	while(n == 0){
        	n = read(namedpipe_fd, str1, 800); 
        }
        if(n == 0){
        	continue;
        }
        if(n != 0){
        	workers_done++;
        }


        char query[100][10][20];
        stringstream ss(str1);
        string to;
        int i=0;
	  	while(getline(ss,to,'\n')){
		  	stringstream ss2(to);
		  	string to2;
			int j=0;
			while(getline(ss2,to2,' ')){
				strcpy(query[i][j], to2.c_str());
				j++;
			}
			num_columns = j;
		  	i++;
		}



		for(int z=0; z<=i; z++){
			for(int j=0; j<=num_columns; j++){
				strcpy(query_result[query_result_num+z][j],query[z][j]);
			}
		}


		query_result_num += i;


		if(isSorted){
			insertionSort(query_result, query_result_num, sorting_index, num_columns);
		}

		close(namedpipe_fd);


		if(workers_done == num_workers){
  			break;
  		} 

    } 


    if(isSorted){
    	if(strcmp(sort_value, "descend") == 0){
    		print_query_reverse(query_result, query_result_num, num_columns);
    		exit(0);
    		reverse(begin(query_result), end(query_result));
    	}
    }

    print_query(query_result, query_result_num, num_columns);


    exit(0);
}