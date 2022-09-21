import apache_beam as beam
import pandas as pd

pipeline = beam.Pipeline()

def apagar_coluna(df):
    df = pd.read_csv("/home/ruckert/testes/age_gender.csv")
    data = df.drop(columns=['img_name', 'pixels'])
    return data

def df_para_dicionario(data):
    d = dict([(i, [a, b]) for i, a, b in zip(
        data['age'], data['ethnicity'], data['gender'])])
    print(d)

transformacao = (
    pipeline
    |"Leitura do arquivo" >> beam.io.ReadFromText("/home/ruckert/testes/age_gender.csv", skip_header_lines=1)
    |"Apagar colunas" >> beam.Map(apagar_coluna)
    |"Transformar em dicionario" >> beam.Map(df_para_dicionario)
    |"Escrever resultado" >> beam.io.WriteToText("/home/ruckert/testes/resultados.txt")
    |"Mostrar resultados" >> beam.Map(print)
)

pipeline.run()
