#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main (int argc, char** args)
{
  int task_id;
  int total_tasks;
  long long int n;
  long long int i;

  double l_sum, x, h;

  task_id = atoi(args[1]);
  total_tasks = atoi(args[2]);
  n = atoll(args[3]);

  fprintf(stderr, "task_id=%d total_tasks=%d n=%lld\n", task_id, total_tasks, n);

  h = 1.0/n;

  l_sum = 0.0;

  for (i = task_id; i < n; i += total_tasks)
  {
    x = (i + 0.5)*h;
    l_sum += 4.0/(1.0 + x*x);
  }

  l_sum *= h;

  printf("%0.12g\n", l_sum);

  return 0;
}
