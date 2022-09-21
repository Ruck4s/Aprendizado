# O arquivo utilizado para esse teste é grande para ser posto no repositório, tentarei achar um arquivo menor para o teste.
# Esse teste foi feito em ambiente local, por isso, modificar o caminho de leitura de arquivo de acordo com cada projeto.
# Necessário baixar as bibliotecas Apache_beam e Pandas, com o comando: pip install (NOME DA BIBLIOTECA).

import apache_beam as beam
import pandas as pd

pipeline = beam.Pipeline()

def apagar_linhas(df):
    df = pd.read_csv("/home/ruckert/testes/age_gender.csv")
    df_remove = df[df.index > 1000].index
    new_df = df.drop(df_remove)
    return new_df

def apagar_coluna(new_df):
    df = new_df
    df_final = df.drop(columns=['img_name', 'pixels'])
    return df_final

def df_para_dicionario(df_final):
    d = dict([(i, [a, b]) for i, a, b in zip(
        df_final['age'], df_final['ethnicity'], df_final['gender'])])
    print(d)

transformacao = (
    pipeline
    |"Leitura do arquivo" >> beam.io.ReadFromText("/home/ruckert/testes/age_gender.csv", skip_header_lines=1)
    |"Apaga linhas" >> beam.Map(apagar_linhas)
    |"Apagar colunas" >> beam.Map(apagar_coluna)
    |"Transformar em dicionario" >> beam.Map(df_para_dicionario)
    |"Escrever resultado" >> beam.io.WriteToText("/home/ruckert/testes/resultados.txt")
    |"Mostrar resultados" >> beam.Map(print) #OPCIONAL
)

pipeline.run()
