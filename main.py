from apache_beam.transforms.core import GroupByKey
import apache_beam as beam
from apache_beam.io import ReadFromText 
from apache_beam.options.pipeline_options import PipelineOptions

def converte_string_to_lista (elem, delim=';'):
  return elem.split(delim)

def filtra_elem_nao_nulo (elem):
  return not (elem[1] == "" or elem[2] == "")

def select_uf_date_casos (elem):
  try:
    valor = float(elem[2])
  except ValueError:
    print("Elemento : " + elem[2])
  return (elem[5] + "-" + elem[1][:7], float(elem[2]))

def filtra_chuvas_invalido (elem):
  return not (float(elem[1]) < 0)

def select_uf_date_chuvas (elem):
  return (elem[2] + "-" + elem[0][:7], float(elem[1]))

def filtra_dicionario_invalido (elem):
  chave, dicion = elem
  return all([dicion['chuvas'], dicion['dengue']])

def convert_lista_to_string(elem):
  chave, dicion = elem
  return chave.replace("-", ",") + "," + str(round(dicion['chuvas'][0],1)) + "," + str(dicion['dengue'][0])

pp_options = PipelineOptions(argv = None)
pipeline = beam.Pipeline(options = pp_options)

dengue = (
    pipeline
    | "passo D1" >> ReadFromText('dengue/casos_dengue.txt', skip_header_lines=1)
    | "passo D2" >> beam.Map(converte_string_to_lista, delim='|')
    | "passo D3" >> beam.Filter(filtra_elem_nao_nulo)
    | "passo D4" >> beam.Map(select_uf_date_casos)
    | "passo D5" >> beam.CombinePerKey(sum)
    #| "passo D6" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "passo C1" >> ReadFromText('dengue/chuvas.csv', skip_header_lines=1)
    | "passo C2" >> beam.Map(converte_string_to_lista, delim=',')
    | "passo C3" >> beam.Filter(filtra_chuvas_invalido)
    | "passo C4" >> beam.Map(select_uf_date_chuvas)
    | "passo C5" >> beam.CombinePerKey(sum)
    #| "passo C6" >> beam.Map(print)
)

resultado = (
    # (chuvas, dengue)
    #| "passo 1" >> beam.Flatten()
    #| "passo 2" >> beam.GroupByKey()
    ({ 'chuvas': chuvas, 'dengue': dengue})
    | "passo 2" >> beam.CoGroupByKey()
    | "passo 3" >> beam.Filter(filtra_dicionario_invalido)
    | "passo 4" >> beam.Map(convert_lista_to_string)
    | "passo 5" >> beam.Map(print)
)

pipeline.run()
