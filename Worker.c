#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h> 
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h> 
#include <iostream>
#include <sstream>

using namespace std;



int is_address(char str[40]){
	for(int i=0; i<strlen(str); i++){
		if(str[i] == '/'){
			return 1;
		}
	}
	return 0;
}


int main(int argc, char **argv){

	
	char inbuf[40]; 
	int nbytes;
	int index = 0;
	int index_result = 0;
	
	char arguments[10][40];
	
	int pipe[2];
	pipe[0] = atoi(argv[1]);
	char namedpipe[20];


	int retval = fcntl( pipe[0], F_SETFL, fcntl(pipe[0], F_GETFL) | O_NONBLOCK);	
	while ((nbytes = read(pipe[0], inbuf, 40)) > 0) {
		strcpy(arguments[index], inbuf);
		index++;
	}



	int last_worker = 0;


	if (nbytes != -1) 
		exit(2); 

	
	
	int fd, sz; 
	char *c = (char *) calloc(100, sizeof(char)); 
	char address[50];

	int file_counter=0;

	int num_col = 0;
	char result[5000] = "";

	char rest_of_address[5][40];
	int num_files =0;
	while(1){
		if(is_address(arguments[index-2-num_files]) == 0){
			break;
		}
		strcpy(rest_of_address[num_files], arguments[index-2-num_files]);
		num_files++;
	}

	
while(1){

	int fd, sz; 
	int index_result = 0;

	if(file_counter > num_files){
		break;
	}


	char fileAddress[40];
	char filter_result[20][100];

	if(file_counter>0 && last_worker==0){
		break;
	}
	if(last_worker == 0){
		strcpy(fileAddress, arguments[0]);
	}
	else{
		strcpy(fileAddress, rest_of_address[file_counter-1]);
	}


	fd = open(fileAddress, O_RDONLY); 
	if (fd < 0) { perror("r1"); exit(1); } 

	struct stat st;
	stat(fileAddress, &st);
	int filesize = st.st_size;

	char bufferFile[filesize];

	sz = read(fd, bufferFile, filesize); 
	bufferFile[sz] = '\0'; 

	
	char * pch;
	char * temp;
	char * temp2;
	char query_part[10][10];
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

	

	int filter_indexes[10];
	char filter_values[10][10];
	int index_filter_values = 0;
	for(int z =0; z<atoi(arguments[1]); z++){
		for(int j =0; j<i; j++){
			if(strcmp(query_header[j] , arguments[(z+1)*2]) == 0){
				filter_indexes[index_filter_values] = j;
				strcpy(filter_values[index_filter_values], arguments[(z+1)*2+1]);
				index_filter_values++;
			}
		}
	}


	if(strcmp(arguments[index-1], "1") == 0){
		num_col = index_filter_values;
		last_worker = 1;
	}


	stringstream ss3(bufferFile);
	getline(ss3,to,'\n');
  	while(getline(ss3,to,'\n')){
	  	stringstream ss4(to);
	  	string to2;
		int j=0;
		while(getline(ss4,to2,' ')){
			strcpy(query_part[j], to2.c_str());
			j++;
		}
		int flag_filter_out = 0;
		for(int j =0; j<index_filter_values; j++){
			if(strcmp(filter_values[j], query_part[filter_indexes[j]]) != 0){
				flag_filter_out = 1;
			}
		}
		if(!flag_filter_out){
			strcpy(filter_result[index_result], to.c_str());
			index_result++;
		}
	  	i++;
	}




	
	for(int i=0; i<index_result ; i++){
		strcat(result, filter_result[i]);
		strcat(result, "\n");
	}
	result[strlen(result)-1] = '\0';
	strcat(result, "\n");
	file_counter++;
	close(fd);
}


	result[strlen(result)-1] = '\0';

	strcpy(namedpipe , "/tmp/myfifo");
	char myfifo_temp[40];
	strcpy(myfifo_temp, namedpipe);
	strcat(myfifo_temp, to_string(atoi(argv[3])).c_str());
	int namedpipe_fd = open(myfifo_temp, O_WRONLY, 0666);

	write(namedpipe_fd, result, strlen(result)+1);
	close(namedpipe_fd); 
	
	exit(0);
	
}