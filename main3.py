from time import sleep

from contextlib import contextmanager

import os
import re

import cx_Oracle


dir = '~/file.txt'


try:

  os.system(f"echo 'Hello! Python is the best programming language.' > {dir}")
  log = os.popen(f"cat {dir}").read()

  print("arquivo carregado na variavel")

finally:

  print("closed")


print(log) 



