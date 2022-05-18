from time import sleep
from contextlib import contextmanager
import os
import re
import cx_Oracle


dir = '/informatica/etl/home/bcc/workflowlogs/'

file_intra = "wf_carrega_fatos_intra_hora_ID_PERIODO.log"

file_billlog = "wf_carrega_fatos_diarias.log"

file_esales =  "wf_brc_ft_mdi_chamadas_esales.log"

#so mudar essa linha pra cada arquivo 
file = file_intra  

print(f'Arquivo {file} selecionado \n')  

try:

  log = os.popen(f"cat {dir}{file}").read()

  print("arquivo carregado na variavel")

finally:
  print("closed")

print(log) 


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

conbi = cx_Oracle.connect(user="DM_TELEFONIA", password="DM_TELEFONIA", dsn="BDRJBI",

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
