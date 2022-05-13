import paramiko

from time import sleep

from contextlib import contextmanager

import re

import cx_Oracle

 

host = '10.5.9.41'

username = 'v'

password = 'b'

 

ssh = paramiko.SSHClient()

 

ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

 

dir = '/informatica/etl/home/bcc/workflowlogs/'

file_intra = "wf_carrega_fatos_intra_hora_ID_PERIODO.log"

file_billlog = "wf_carrega_fatos_diarias.log"

file_esales =  "wf_brc_ft_mdi_chamadas_esales.log"

 

n = input("Qual carga deseja rodar? \n 1 - Dagent/Hsplit \n 2 - Billlog \n 3 - Esales: \n")

 

if n == "1":

    file = file_intra

elif n == "2":

    file =  file_billlog

elif n == "3":

    file =  file_esales

else:

    raise Exception("opcao invalida")

   

print(f'Arquivo {file} selecionado \n')  

 

print("Buscando Arquivo")

 

try:

 

    print("creating connection")

    ssh.connect(host, username=username, password=password)

    print("connected")

    stdin, stdout, stderr = ssh.exec_command(f'cat {dir}{file}')

    log = stdout.read()

    print("arquivo carregado na variavel")

finally:

 

    print("closing connection")

    ssh.close()

    print("closed")

 

print("decodificando bytes para str")

log = log.decode('utf-8')

 

#PEGA ID_PERIODO

 

a = 'Use override value ['

aa = '] for session parameter:[$ParamID_PERIODO'

 

regexp = r"(?<="+re.escape(a)+")(.*)(?="+re.escape(aa)+")"

 

id_periodo=re.search(regexp,log).group(1)

print(f'ID_PERIODO: {id_periodo}')

 

#PEGA ID_RUN

b = 'started with run id ['

regexp2 = r"(?<="+re.escape(b)+")(.*)(?=], run in)"

 

run_id=re.search(regexp2,log).group(1)

print(f'RUN_ID: {run_id}')

 

print("Conectando no banco")

conbi = cx_Oracle.connect(user="DM", password="DM", dsn="BD",

                               encoding="UTF-8")      




cursor = conbi.cursor()  

cursor.execute(f"SELECT count(*) FROM DM_TELEFONIA.DI_LOG_PCENTER WHERE ID_RUN = {run_id}")

count = cursor.fetchone()[0]

cursor.close()

 

cur = conbi.cursor()

if count != 0:

    print("ID_RUN ja existe na tabela")

else:

   

    print("Inserindo na tabela")

    cur = conbi.cursor()

    cur.execute(f"insert into DM_TELEFONIA.DI_LOG_PCENTER values ({run_id},{id_periodo}, sysdate)")

    conbi.commit()

 

    print("Insert finalizado, encerrando conexao")

 

cur.close()

conbi.close()

print("programa finalizado")

sleep(2)

