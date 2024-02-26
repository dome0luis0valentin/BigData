import math
import re
from drive.MyDrive.MRE import Job

root_path = '/content/drive/MyDrive/Colab/A1/A1/'

# Input
inputDir = root_path + "input/"

# Outputs
outputDir = root_path + "output/"
output_cleaner = outputDir + 'output_cleaner'
output_tf = outputDir + 'output_tf'
output_tf_idf = outputDir + 'output_tf_idf'
output_d = outputDir + 'output_d'

""" Job Cleaner """

def fmap_cleaner(key, values, context):
  """ Procesa los documentos de recetas, eliminando caracteres no deseados
      y enviando el texto limpio junto con su identificador de documento
      al proceso de reducción.

      Parametros:
        key (str): La clave que identifica el documento.
        values (str): El texto del documento como una cadena de caracteres.
  """
  id_doc = key
  texto = values
  texto_limpio = ""

  # La lista de caracteres permitidos
  caracteres_permitidos = r"A-Za-z0-9áéíóúüÁÉÍÓÚÜñÑ"

  #  Filtro
  if "Ingredientes:" not in texto:
    palabras = texto.split(" ")
    for palabra in palabras:
      if (palabra != ',' and palabra != ' '):  # Porque hay ',' entre dos espacios
        palabra_limpia = re.sub(f"[^({caracteres_permitidos})]", "", palabra)

        # Concatenamos las palabras encontradas en una sola cadena
        texto_limpio += palabra_limpia + " "

    context.write(id_doc, texto_limpio)


def fred_cleaner(key, values, context):
  """ Combina los fragmentos de texto correspondientes a un documento

    Parametros:
      key (str): La clave que identifica el documento.
      values (str): El texto del documento como una cadena de caracteres.
  """
  documento_completo = ""

  # Unifico los textos del documento
  for v in values:
    documento_completo += v + " "

  context.write(key, documento_completo)  # <id_doc, documento>

###############################################

""" Job TF """

def fmap_tf(key, values, context):
  """ Crea una representación de los pares (documento, palabra) y la frecuencia
      de esa palabra en ese documento.

      Parametros:
        key (str): La clave que identifica el documento.
        values (str): El texto del documento como una cadena de caracteres.
  """
  id_doc = key
  palabras = values.split(" ")
  cantidad_palabras = len(palabras)

  for palabra in palabras:
    context.write((id_doc, palabra), (palabras.count(palabra), cantidad_palabras))


def fred_tf(key, values, context):
  """ Calcula la frecuencia de términos (TF) de un término en un documento.

    Parámetros:
      key (tuple): Una tupla que contiene el identificador del documento y la palabra.
      values (list): Una lista de tuplas que contienen la cantidad de veces que aparece el término en el documento
      y la cantidad total de palabras en el documento.

    La función toma los valores de cada tupla en la lista de valores y calcula la frecuencia de términos (TF) normalizada
    dividiendo la cantidad de veces que aparece el término en el documento entre la cantidad total de palabras en el documento.
  """

  cantidad_termino_en_el_documento = 0  # ft(d)
  cantidad_palabras_documento = 0  # #d

  #Como values contiene un solo elemento, solo se itera una vez
  for v in values:
    cantidad_termino_en_el_documento = int(v[0])
    cantidad_palabras_documento = int(v[1])

  # <(id_doc, termino), TF(t,d)>
  context.write(key, "{:.6f}".format(float(cantidad_termino_en_el_documento)/float(cantidad_palabras_documento)))

###############################################

""" Job D """

def fmap_d(key, values, context):
  """ El valor final es contar la cantidad de documentos
  """
  context.write(1, 1)

def fcom_d(key, values, context):
  """
  Esta función reduce el tráfico de la red, precalculando algunos valores que
  utilizará la función red
  """
  d = 0

  for v in values:
    d += v

  context.write(1, d)

def fred_d(key, values, context):
  """ Calcula la cantidad total de documentos procesados.

    Parámetros:
      key (str): El término clave, que en este caso generalmente es 1 para que le lleguen todos los documentos.
      values (list): Una lista de acumuladores, donde cada valor es 1, que se utiliza para contar documentos.
  """
  d = 0  # D

  # Unifico los textos del documento
  for v in values:
    d += v

  context.write("D: ", d)  # <"D: ", #D>

###############################################

""" Job TF_IDF """

def fmap_tf_idf(key, values, context):
  """ Prepara los datos para calcular el valor TF-IDF de un término.

    Parámetros:
      key (str): El identificador del documento.
      values (str): Una cadena que contiene información sobre el término y su valor TF.

    La función emite un par clave-valor en el contexto, donde la clave es el término
    y el valor es el valor TF.
    """
  texto = values.split("\t")
  termino = texto[0]
  tf = texto[1]

  #context.write(termino, tf)  # <termino, TF(termino, d)>
  context.write(termino, (tf, 1))  # <termino, TF(termino, d), cantidad de docs en los que aparece>

def fcom_tf_idf(key, values, context):
  """ Realiza parte del calculo del valor TF-IDF de un término en un conjunto de documentos.
  Su objetivo es preprocesar determinada parte de los datos, para finalmente calcular el TF-IDF

    Parámetros:
      key (str): El término que se está procesando.
      values (list): Una lista de los valores con el par:
        - TF para el término en diferentes documentos.
        - Cantidad de documentos en el que aparece el término.

    Para realizar este cálculo, se recupera el valor total de documentos (#D) de los parámetros del job.
    Luego, se calcula la cantidad de documentos que contienen el término (D(t)), así como la sumatoria de los valores TF
    para el término en esos documentos.

    Como salida se obtiene la clave, que es el termino, un par (tf, cant_doc), donde tf es la frecuencia en N documentos
    y cant_doc es la cantidad de documentos en donde se calculo ese termino.
  """
  d = context["d"]  # Recupero el valor #D de los parametros del job
  cantidad_documentos_con_el_termino = 0  # D(t)
  sumatoria_tf = 0

  # Unifico los textos del documento
  for v in values:
    #cantidad_documentos_con_el_termino += 1
    sumatoria_tf += float(v[0])
    cantidad_documentos_con_el_termino += v[1]


  context.write(key, (sumatoria_tf, cantidad_documentos_con_el_termino))

def fred_tf_idf(key, values, context):
  """ Calcula el valor TF-IDF de un término en un conjunto de documentos.

    Parámetros:
      key (str): El término que se está procesando.
      values (list): Una lista de los valores de TF para el término en diferentes documentos.

    La función utiliza los valores TF para un término en varios documentos y calcula su valor TF-IDF.
    Para realizar este cálculo, se recupera el valor total de documentos (#D) de los parámetros del job.
    Luego, se calcula la cantidad de documentos que contienen el término (D(t)), así como la sumatoria de los valores TF
    para el término en esos documentos. Finalmente, se aplica la fórmula del TF-IDF y se emite un par clave-valor
    en el contexto, donde la clave es el término y el valor es el valor TF-IDF calculado.
  """
  d = context["d"]  # Recupero el valor #D de los parametros del job
  cantidad_documentos_con_el_termino = 0  # D(t)
  sumatoria_tf = 0

  # Unifico los textos del documento
  for v in values:
    #cantidad_documentos_con_el_termino += 1
    sumatoria_tf += float(v[0])
    cantidad_documentos_con_el_termino += v[1]


  # Calcula el valor TF-IDF
  tf_idf = (math.log10(int(d) / int(cantidad_documentos_con_el_termino))) * sumatoria_tf

  context.write(key, "{:.6f}".format(tf_idf))  # <termino, TF-IDF(termino)>


###############################################

# Job Cleaner
job_cleaner = Job(inputDir, output_cleaner, fmap_cleaner, fred_cleaner)
job_cleaner.setCombiner(fred_cleaner)
job_cleaner.waitForCompletion()

# Job TF
job_tf = Job(output_cleaner, output_tf, fmap_tf, fred_tf)

# Job D
job_d = Job(output_cleaner, output_d, fmap_d, fred_d)
job_d.setCombiner(fcom_d)

job_tf.waitForCompletion()
job_d.waitForCompletion()

# Para recuperar el valor #D de la salida del Job D
d = 0

# Abre el archivo en modo lectura
with open(output_d + "/output.txt", "r") as file:
  content = file.read()

  parts = content.split('\t')

  if len(parts) >= 2:
      d = parts[1].strip()  # Extrae el valor #D

params = {"d": d}

# Job TF_IDF
job_tf_idf = Job(output_tf, output_tf_idf, fmap_tf_idf, fred_tf_idf)
job_tf_idf.setCombiner(fcom_tf_idf)
job_tf_idf.setParams(params)
job_tf_idf.waitForCompletion()