#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <math.h>
#include <iostream>
#include <iomanip>
#include <random>

using namespace std;

double distance(double x1, double y1, double x2, double y2);
void initial_placement(double *points_x, double *points_y, int num_points, int seed);
double cpuSecond();

int main(int argc, char **argv)
{
  int num_points, num_clusters, num_iter, seed;
  int my_num_points;
  double *points_x, *points_y, *centroids_x, *centroids_y, *sum_x, *sum_y;
  int *assignment;                        // for each point i assignment[i] is the cluster i was assigned to
  int *num_assigned, *num_assigned_local; // for each cluster i num_assigned[i] is the number of points assigned to cluster i
  double tot_dist;
  int best;
  double min_dist, dist;
  int myrank, num_workers, name_len;
  char name[MPI_MAX_PROCESSOR_NAME];
  double begin = 0.0, duration;

  MPI_Init(&argc, &argv);

  MPI_Comm_size(MPI_COMM_WORLD, &num_workers);
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
  MPI_Get_processor_name(name, &name_len);
  MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_ARE_FATAL);

  // parse arguments
  if (argc != 5)
  {
    cout << "Usage: " << argv[0] << " <num points> <num clusters> <num iterations> <seed>" << endl;
    exit(-1);
  }

  num_points = atoi(argv[1]);
  num_clusters = atoi(argv[2]);
  num_iter = atoi(argv[3]);
  seed = atoi(argv[4]);

  // allocate memory
  if (myrank == 0)
    my_num_points = num_points / num_workers + num_points % num_workers;
  else
    my_num_points = num_points / num_workers;
  points_x = new double[my_num_points];
  points_y = new double[my_num_points];
  centroids_x = new double[num_clusters];
  centroids_y = new double[num_clusters];
  sum_x = new double[num_clusters];
  sum_y = new double[num_clusters];
  assignment = new int[my_num_points];
  num_assigned = new int[num_clusters];
  num_assigned_local = new int[num_clusters];

  // calculate initial placement (random)
  if (myrank == 0)
  {
    double *px = new double[num_points];
    double *py = new double[num_points];
    initial_placement(px, py, num_points, seed);
    for (int i = 1; i < num_workers; i++)
    {
      MPI_Send(px + my_num_points + (i - 1) * (num_points / num_workers), num_points / num_workers, MPI_DOUBLE, i, 1, MPI_COMM_WORLD);
      MPI_Send(py + my_num_points + (i - 1) * (num_points / num_workers), num_points / num_workers, MPI_DOUBLE, i, 1, MPI_COMM_WORLD);
    }
    for (int i = 0; i < my_num_points; i++)
    {
      points_x[i] = px[i];
      points_y[i] = py[i];
    }
    delete[] px;
    delete[] py;
  }
  else
  {
    MPI_Recv(points_x, my_num_points, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD, NULL);
    MPI_Recv(points_y, my_num_points, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD, NULL);
  }

  cout << fixed << setprecision(8);

  if (myrank == 0)
  {
    begin = cpuSecond();

    // place centroids as first points
    for (int i = 0; i < num_clusters; i++)
    {
      centroids_x[i] = points_x[i];
      centroids_y[i] = points_y[i];
    }
  }

  // share centroids
  MPI_Bcast(centroids_x, num_clusters, MPI_DOUBLE, 0, MPI_COMM_WORLD);
  MPI_Bcast(centroids_y, num_clusters, MPI_DOUBLE, 0, MPI_COMM_WORLD);

  // iterate
  for (int i = 0; i < num_iter; i++)
  {
    // cout << "Process " << myrank << ": Iteration " << i << endl;

    // assign points to clusters and count number of points assigned
    for (int c = 0; c < num_clusters; c++)
      num_assigned_local[c] = 0;
    for (int p = 0; p < my_num_points; p++)
    {
      best = 0;
      min_dist = 10.0;
      for (int c = 0; c < num_clusters; c++)
      {
        dist = distance(points_x[p], points_y[p], centroids_x[c], centroids_y[c]);
        if (dist < min_dist)
        {
          best = c;
          min_dist = dist;
        }
      }
      assignment[p] = best;
      num_assigned_local[best]++;
    }
    // calculate new centroids
    for (int c = 0; c < num_clusters; c++)
    {
      sum_x[c] = 0.0;
      sum_y[c] = 0.0;
    }
    for (int p = 0; p < my_num_points; p++)
    {
      sum_x[assignment[p]] += points_x[p];
      sum_y[assignment[p]] += points_y[p];
    }
    MPI_Allreduce(sum_x, centroids_x, num_clusters, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(sum_y, centroids_y, num_clusters, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(num_assigned_local, num_assigned, num_clusters, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    for (int c = 0; c < num_clusters; c++)
    {
      centroids_x[c] = centroids_x[c] / num_assigned[c];
      centroids_y[c] = centroids_y[c] / num_assigned[c];
    }

    // calculate and print total distance
    dist = 0.0;
    for (int p = 0; p < my_num_points; p++)
    {
      dist += distance(points_x[p], points_y[p], centroids_x[assignment[p]], centroids_y[assignment[p]]);
    }
    MPI_Reduce(&dist, &tot_dist, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    if (myrank == 0)
    {
      cout << "Process " << myrank << ": distance is " << tot_dist << endl;
    }
  }

  if (myrank == 0)
  {
    duration = cpuSecond() - begin;

    cout << "Process " << myrank << ": It took " << duration << "s to run the algorithm" << endl;
  }

  delete[] points_x;
  delete[] points_y;
  delete[] centroids_x;
  delete[] centroids_y;
  delete[] sum_x;
  delete[] sum_y;
  delete[] assignment;
  delete[] num_assigned;
  delete[] num_assigned_local;

  MPI_Finalize();
}

double distance(double x1, double y1, double x2, double y2)
{
  return sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
}

void initial_placement(double *points_x, double *points_y, int num_points, int seed)
{
  uniform_real_distribution<> dis(-1.0, 1.0);
  mt19937 gen(seed);

  for (int i = 0; i < num_points; i++)
  {
    points_x[i] = dis(gen);
    points_y[i] = dis(gen);
  }
}

double cpuSecond()
{
  struct timeval tp;
  gettimeofday(&tp, NULL);
  return ((double)tp.tv_sec + (double)tp.tv_usec * 1.e-6);
}
