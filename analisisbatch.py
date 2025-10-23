
#Dataset tomado: https://www.kaggle.com/datasets/miadul/lifestyle-and-health-risk-prediction


#libreria para poder ejecutar
from pyspark.sql import SparkSession, functions as F


# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('AnalisisBatch').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)


#mostrar el contenido
Print('Contenido del Dataset, 5000 registros')
df.show(5)
print("\n==============================")


#Renombrar columnas
df = (df
    .withColumnRenamed('age', 'EDAD')
    .withColumnRenamed('weight', 'PESO')
    .withColumnRenamed('height', 'ALTURA')
    .withColumnRenamed('exercise', 'EJERCICIO')
    .withColumnRenamed('sleep', 'SUEÑO')
    .withColumnRenamed('sugar_intake', 'CONSU_AZUCAR')
    .withColumnRenamed('smoking', 'FUMA')
    .withColumnRenamed('alcohol', 'ALCOHOL')
    .withColumnRenamed('married', 'CASAD@')
    .withColumnRenamed('profession', 'PROFESION')
    .withColumnRenamed('bmi', 'IMC')
    .withColumnRenamed('health_risk', 'RIESGO')
)
print('Columnas renombradas')
df.show(5)
print("\n==============================")


#Muestra la información de como esta conformado el dataframe
print('Conformación del dataset')
df.printSchema()
print("\n==============================")


#verificación de valores nulos
nulos = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
print('Cantidad de valores nulos por columna')
nulos.show()
print("\n==============================")


#Promedios para riesgo alto
altoriesgo = df.filter(F.col('RIESGO') == 'high')

promedios = altoriesgo.select(
    F.round(F.avg('EDAD'), 2).alias('Promedio_EDAD'),
    F.round(F.avg('IMC'), 2).alias('Promedio_IMC'),
    F.round(F.avg('SUEÑO'), 2).alias('Promedio_SUEÑO')
)
print('Promedio de EDAD, índice de masa corporal IMC y horas de sueño para personas en riesgo alto')
print(promedios)
print("\n==============================")


#Profesiones en riesgo alto
altoriesgo = df.filter(F.col('RIESGO') == 'high')

profesiones_alto = (altoriesgo.groupBy('PROFESION').count().orderBy(F.desc('count')))

print('Cantidad de personas en riesgo alto por profesión de 5000 registros')
profesiones_alto.show()
print("\n==============================")



#Sumadores en riesgo alto y bajo
fumadores = df.filter(F.col('FUMA') == 'yes')

num_fumadores = fumadores.count()
print(f'Número de personas que fuman: {num_fumadores}, de 5000 registros.') 

riesgo_fumadores = (fumadores.groupBy('RIESGO').count().orderBy(F.desc('count')))

print('Fumadores vs nivel de riesgo')
riesgo_fumadores.show()
print("\n==============================")


#Personas que fuman y consumen alcohol vs riesgo
consumo = df.filter((F.col('FUMA') == 'Yes') & (F.col('ALCOHOL') == 'Yes')

promedio_alto = (consumo.filter(F.col('RIESGO') == 'high')
                   .agg(F.round(F.avg('EDAD'), 2).alias('Promedio_EDAD'),
                        F.round(F.avg('SUEÑO'), 2).alias('Promedio_SUEÑO')))

promedio_bajo = (consumo.filter(F.col('RIESGO') == 'low')
                   .agg(F.round(F.avg('EDAD'), 2).alias('Promedio_EDAD'),
                        F.round(F.avg('SUEÑO'), 2).alias('Promedio_SUEÑO')))

print('Promedios de edad y horas de sueño para personas con RIESGO BAJO:')
promedio_bajo.show()

print("\n==============================")

print('Promedios de edad y horas de sueño para personas con RIESGO ALTO:')
promedio_alto.show()

print("\n==============================")




# Guardar resultados

salida = "/home/vboxuser/resultados"

df.write.mode(
altoriesgo.write.mode("overwrite").csv(f"{ruta_salida}/altoriesgo")
profesiones_alto.write.mode("overwrite").csv(f"{ruta_salida}/profesiones_alto")
num_fumadores.write.mode("overwrite").csv(f"{ruta_salida}/num_fumadores")
riesgo_fumadores.write.mode("overwrite").csv(f"{ruta_salida}/riesgo_fumadores")
consumo.write.mode("overwrite").csv(f"{ruta_salida}/consumo")
promedio_alto.write.mode("overwrite").csv(f"{ruta_salida}/promedio_alto")
promedio_bajo.write.mode("overwrite").csv(f"{ruta_salida}/promedio_bajo")
print("Todos los resultados fueron almacenados correctamente en:")
print(f"   {ruta_salida}\n"))











