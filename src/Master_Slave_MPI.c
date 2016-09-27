/*
 ============================================================================
 Name        : MasterSlaveMPI.c
 Author      : Shailesh Vajpayee
 Version     : 1.0.1.2
 Copyright   : All rights reserved
 Description : Sorting using Master Slave Approach and MPI
 Command Line Arguments: The length of the array (1mill,10mill,100mill).
 ============================================================================
 */

#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

//This is the main function of the file.
//length of the array has to be given as the command line argument.

int main(int argc, char *argv[]) {
	MPI_Init(&argc, &argv);

	int my_rank; /* rank of process */
	int num_procs;/* number of processes */
	/* start up MPI */

	/* find out process rank */
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	/* find out number of processes */
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	num_procs = num_procs - 1;
	clock_t begin, end;
	double time_spent;
	if (argc <= 1) {
		printf("Please enter the length of array in argument(1000000,10000000 or 100000000)");
		exit(1);
	}
	int length = atoi(argv[1]);
	if (num_procs == 0) {
		printf("only one process\n");
		clock_t m_begin, m_end;
		double m_time_spent;
		m_begin = clock();
		int *list1 = malloc(length * sizeof(int));
		for (int i = 0; i < length; i++) {
			list1[i] = rand() % 100;
//			printf("%d\n", list1[i]);
		}
		int i, j, temp;
		for (i = 1; i < length; i++) {
			temp = list1[i];
			for (j = i - 1; (j >= 0) && (list1[j] > temp); j--) {
				list1[j + 1] = list1[j];
				list1[j] = temp;
			}
		}
		m_end = clock();
		m_time_spent = m_end - m_begin;
		printf("Time taken for 1 machine is %f ns\n", m_time_spent);
//		for (int index = 0; index < length; index++) {
//			printf("%d\n", list1[index]);
//		}
		MPI_Finalize();

	} else {
		begin = clock();
		if (my_rank == 0) { //if master
			printf("Master has begun\n");
			printf("length of array is %d\n", length);
			int *list = malloc(length * sizeof(int));
			int sub_length = length / num_procs;
			int *div_list = malloc(sub_length * sizeof(int));
			clock_t mbegin, mend;
			double mtime_spent;
			begin = clock();
			MPI_Status status;
			srand(time(NULL));

			clock_t m1begin, m1end;
			double m1time_spent;
			m1begin = clock();
			for (int i = 0; i < length; i++) {
				list[i] = rand() % 100;
//			printf("%d\n", list[i]);
			}
			m1end = clock();
			m1time_spent = m1end - m1begin;
			printf("\nTime taken to initialize list is %f ns\n", m1time_spent);
			int l = 0;
			clock_t m2begin, m2end, m4end, m4begin, m5begin, m5end;
			double m2time_spent, m4time_spent, m5time_spent;
			m2begin = clock();
			for (int i = 0; i < num_procs; i++) {
				for (int j = 0; j < sub_length; j++) {
					div_list[j] = list[l];
					l++;
				}
				MPI_Send(div_list, sub_length, MPI_INT, i + 1, 0,
				MPI_COMM_WORLD);
			}
			m2end = clock();
			m2time_spent = m2end - m2begin;
			printf("\nTime taken to send list to processes is %f ns\n",
					m2time_spent);
			free(div_list);
			free(list);
			l = 0;
			int *output = malloc(length * sizeof(int));

			int *div_list2 = malloc(sub_length * sizeof(int));
			int **test = (int **) malloc(num_procs * sizeof(int *));
			for (int i = 0; i < num_procs; i++) {
				test[i] = (int *) malloc(sub_length * sizeof(int));
			}

			m5begin = clock();
			for (int i = 0; i < num_procs; i++) {

				MPI_Recv(div_list2, sub_length, MPI_INT, i + 1, 0,
				MPI_COMM_WORLD, &status);

				for (int j = 0; j < sub_length; j++) {
					test[i][j] = div_list2[j];
				}
			}
			m5end = clock();
			m5time_spent = m5end - m5begin;
			printf("Time taken to gather data %f ns\n", m5time_spent);

			m4begin = clock();
			int min;         // minimum in this iteration
			int minposition; // position of the minimum
			int i = 0; //index for output array
			int flag = 0;

			int * cursor = calloc(num_procs, sizeof(int)); // cursor for the 2d array

			if (cursor == NULL) {
				flag = 1;
			} else {
				while (1) {
					min = 10000000;
					minposition = -1; // invalid position

					// Go through the current positions and get the minimum
					for (int j = 0; j < num_procs; ++j) {

						if (cursor[j] < sub_length && // ensure that the cursor is still valid
								test[j][cursor[j]] < min) { // the element is smaller
							min = test[j][cursor[j]];  // save the minimum ...
							minposition = j;             // ... and its position
						}
					}

					// if there is no minimum, then the position will be invalid

					if (minposition == -1)
						break;

					// update the output and the specific cursor
					output[i++] = min;
					cursor[minposition]++;
				}
			}

//		for(int ind = 0; ind < length; ind++){
//			printf("%d\n", output[ind]);
//		}
			free(div_list2);
			free(cursor);
			printf("Master has finished gathering and inserting\n");
			m4end = clock();
			m4time_spent = m4end - m4begin;
			printf("Time spent to insert elements in right order %f ns\n",
					m4time_spent);
			mend = clock();
			mtime_spent = (mend - mbegin);
			printf("time spent by master %f ns\n", mtime_spent);

		}
		if (my_rank > 0) { // if slave
			clock_t sbegin, send;
			double stime_spent;
			printf("Slave %d started\n", my_rank);
			sbegin = clock();
			MPI_Status status;
			int sub_length = length / num_procs;
			int *div_list3 = malloc(sub_length * sizeof(int));

			MPI_Recv(div_list3, sub_length, MPI_INT, MPI_ANY_SOURCE,
			MPI_ANY_TAG,
			MPI_COMM_WORLD, &status);

			int i, j, temp;
			for (i = 1; i < sub_length; i++) {
				temp = div_list3[i];
				for (j = i - 1; (j >= 0) && (div_list3[j] > temp); j--) {
					div_list3[j + 1] = div_list3[j];
					div_list3[j] = temp;
				}
			}

			MPI_Send(div_list3, sub_length, MPI_INT, 0, 0, MPI_COMM_WORLD);
			send = clock();
			stime_spent = (send - sbegin);
			printf("Slave %d finished sorting in time %f ns\n", my_rank,
					stime_spent);
		}
		MPI_Finalize();
		end = clock();
		time_spent = (end - begin);
	}
	return 0;
}

