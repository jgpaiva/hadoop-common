#!/usr/bin/env python
import random

n_files = 300
d="large_random/"


ratio=[1, 10, 25, 50, 75, 90, 99]
repetition=range(100)

for x in ratio:

 for y in repetition:
  record = 'relevant_' + str(x) + '_' + str(y) + '\n'

  with open('../recordList.txt', "a") as fopen:
   fopen.write(record)

  list_files = random.sample(range(n_files), x*n_files/100)

  for f in list_files:
   if f < 10:
    f = '00' + str(f)
   elif f < 100:
    f = '0' + str(f)

   file_name = 'large' + str(f) + '_0_.txt'

   with open(d + file_name, "a") as fopen:
    fopen.write(record)
