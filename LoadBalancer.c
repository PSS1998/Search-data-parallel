#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h> 
#include <fcntl.h> 
#include <sys/stat.h>
#include <unistd.h> 
#include <stdio.h>
#include <dirent.h>
#include <cstring>
#include <sstream>
#include <sys/wait.h>

using namespace std;


int match(const char *s, const char *p, int overlap){
	int c = 0, l = strlen(p);

	while (*s != '\0') {
		if (strncmp(s++, p, l)) continue;
		if (!overlap) s += l - 1;
		c++;
	}
	return c;
}

char* itoa(int val, int base){
	
	static char buf[32] = {0};
	
	int i = 30;
	
	for(; val && i ; --i, val /= base)
	
		buf[i] = "0123456789abcdef"[val % base];
	
	return &buf[i+1];
	
}


int main(){
	
	while(1){


	    char myfifo[20] = "/tmp/myfifo"; 
	  
	    mkfifo(myfifo, 0666);
		
		char input[100] = "";
		int isSorted = 0;
		int num_filter = 0;


		cin.getline(input,sizeof(input));

		
		isSorted = match(input, "ascend", 0) + match(input, "descend", 0);
		num_filter = match(input, "-", 0);
		num_filter = num_filter-1-isSorted;

		
		char * pch;
		char input_array[10][15];	
		int index = 0;
		pch = strtok (input," =-");
		strcpy(input_array[index], pch);
		while (pch != NULL)
		{
			pch = strtok (NULL, " =-");
			if(pch == NULL)
				break;
			index++;
			strcpy(input_array[index], pch);
		}
		for(int i=0; i<=index; i++){
			// printf("%s\n", input_array[i]);
		}

		int num_workers = atoi(input_array[index-2]);


		int len;
        struct dirent *pDirent;
        DIR *pDir;

        pDir = opendir (input_array[index]);
        if (pDir == NULL) {
            printf ("Cannot open directory '%s'\n", input_array[index]);
            return 1;
        }

        char input_files[10][20];
        int index_files = 0;

        while ((pDirent = readdir(pDir)) != NULL) {
        	if(strcmp(pDirent->d_name, "..") == 0 || strcmp(pDirent->d_name, ".") == 0){
        		continue;
        	}
        	strcpy(input_files[index_files], pDirent->d_name);
        	index_files++;
        }
        closedir (pDir);


        int num_args = 10;
		char argv[num_args][40];


		int sort_index = 0;
		if(isSorted){
			char fileAddress[40];
			strcpy(fileAddress, input_array[index]);
			strcat(fileAddress, "/");
			strcat(fileAddress, input_files[0]);
			int file_fd = open(fileAddress, O_RDONLY);
			char bufferFile[100];
			int sz = read(file_fd, bufferFile, 100);
			bufferFile[sz] = '\0';
			char query_header[10][10];
			char header[100];
			stringstream ss(bufferFile);
		  	string to;
		  	getline(ss,to,'\n');
		  	strcpy(header, to.c_str());
		  	stringstream ss2(header);
			int i=0;
			while(getline(ss2,to,' ')){
			  strcpy(query_header[i], to.c_str());
			  i++;
			}
			for(int z=0; z<=i; z++){
				if(strcmp(query_header[z], input_array[index-5]) == 0){
					sort_index = z;
				}
			}
		}


		strcpy(argv[0], "./Presenter");
	    strcpy(argv[1], myfifo);
	    if(isSorted){
	    	strcpy(argv[2], to_string(sort_index).c_str());
	    	strcpy(argv[3], input_array[index-4]);
		}
		strcpy(argv[4], input_array[index-2]);

		char *pCar[30];
		int n = 0; 
		char *tok = strtok(argv[0], " "); 
		while (tok != NULL && n < 30)
		{
		  pCar[n++] = tok;
		  tok = strtok(NULL, " "); 
		}
		tok = strtok(argv[1], " "); 
		while (tok != NULL && n < 30)
		{
		  pCar[n++] = tok;
		  tok = strtok(NULL, " "); 
		}
		if(isSorted){
			tok = strtok(argv[2], " "); 
			while (tok != NULL && n < 30)
			{
			  pCar[n++] = tok;
			  tok = strtok(NULL, " ");
			}
			tok = strtok(argv[3], " "); 
			while (tok != NULL && n < 30)
			{
			  pCar[n++] = tok;
			  tok = strtok(NULL, " ");
			}
		}
		

		tok = strtok(argv[4], " "); 
		while (tok != NULL && n < 30)
		{
		  pCar[n++] = tok;
		  tok = strtok(NULL, " ");
		}
		pCar[n++] = NULL;


		int pid = fork();
		if(pid == 0){
			execvp(pCar[0], pCar);
		}

		
		for(int i=0; i<atoi(input_array[index-2]); i++){
			char myfifo_temp[40];
			strcpy(myfifo_temp, myfifo);
			strcat(myfifo_temp, to_string(i).c_str());
			mkfifo(myfifo_temp, 0666);
		}
		
		for(int i=0; i<atoi(input_array[index-2]); i++){
			
			int last_worker = 0;
			if(i == atoi(input_array[index-2])-1){
				last_worker = 1;
			}
			

			int num_args = 10;
			char argv[num_args][10];

			int p[2], nbytes;
		    if (pipe(p) < 0) 
		        exit(1);


		    strcpy(argv[0], "./Worker");
		    strcpy(argv[1], to_string(p[0]).c_str());
		    strcpy(argv[2], myfifo);
		    strcpy(argv[3], to_string(i).c_str());
		    

			char *pCar[30]; 
			int n = 0; 
			char *tok = strtok(argv[0], " ");
			while (tok != NULL && n < 30)
			{
			  pCar[n++] = tok;
			  tok = strtok(NULL, " "); 
			}
			tok = strtok(argv[1], " ");
			while (tok != NULL && n < 30)
			{
			  pCar[n++] = tok;
			  tok = strtok(NULL, " "); 
			}
			tok = strtok(argv[2], " "); 
			while (tok != NULL && n < 30)
			{
			  pCar[n++] = tok;
			  tok = strtok(NULL, " ");
			}
			tok = strtok(argv[3], " "); 
			while (tok != NULL && n < 30)
			{
			  pCar[n++] = tok;
			  tok = strtok(NULL, " "); 
			}
			pCar[n++] = NULL;


			char * msg1 = input_files[0];
			char fileAddress[40];
			strcpy(fileAddress, input_array[index]);
			strcat(fileAddress, "/");
			strcat(fileAddress, input_files[i]);
			write(p[1], fileAddress, 40);
			write(p[1], to_string(num_filter).c_str(), 40);
			for(int i=0; i<index+1; i++){
				write(p[1], input_array[i], 40);
			}
			if(last_worker){
				if(num_workers < index_files){
					for(int j=0; j<index_files-num_workers; j++){
						char fileAddress[40];
						strcpy(fileAddress, input_array[index]);
						strcat(fileAddress, "/");
						strcat(fileAddress, input_files[i+j+1]);
						write(p[1], fileAddress, 40);
					}
					write(p[1], "1", 5);
				}
				else{
					write(p[1], "0", 5);
				}
			}


			int pid = fork(); 
			 
			if (pid == -1){
				exit(1);
			}

			if (pid == 0){
				int error = execvp(pCar[0], pCar);
			}
			 
			if (pid > 0){
				int status;
				pid_t child_pid, wpid;
				wpid = waitpid(pid,&status,0);
			}
		}

		int status;
		pid_t child_pid, wpid;



	}
	exit(0);

}
