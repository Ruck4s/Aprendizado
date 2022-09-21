# O arquivo utilizado para esse teste está no caminho Dataflow/titanic.csv.
# Esse teste foi feito em ambiente local, por isso, modificar o caminho de leitura de arquivo de acordo com cada projeto.
# Necessário baixar as bibliotecas Apache_beam e Pandas, com o comando: pip install (NOME DA BIBLIOTECA).

import apache_beam as beam
import pandas as pd

pipeline = beam.Pipeline()

#Apaga a coluna "Survived" do arquivo.
def apagar_coluna(df):
    df = pd.read_csv("/home/ruckert/testes/titanic.csv")
    data = df.drop(columns=['Survived'])
    return data

#Transforma o DataFrame "data" em um dicionario com o arquivo modificado.
def df_para_dicionario(data):
    d = dict([(i, [a, b]) for i, a, b in zip(
        data['age'], data['ethnicity'], data['gender'])])
    print(d)

transformacao = (
    pipeline
    |"Leitura do arquivo" >> beam.io.ReadFromText("/home/ruckert/testes/titanic.csv", skip_header_lines=1)
    |"Apagar colunas" >> beam.Map(apagar_coluna)
    |"Transformar em dicionario" >> beam.Map(df_para_dicionario)
    |"Escrever resultado" >> beam.io.WriteToText("/home/ruckert/testes/resultados.txt")
    |"Mostrar resultados" >> beam.Map(print)
)

pipeline.run()
