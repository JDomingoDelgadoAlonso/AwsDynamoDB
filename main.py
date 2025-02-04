from conexion import obtener_conexion
from ejercicios import*

# Obtener el cliente de DynamoDB
dynamodb = obtener_conexion()

# EJERCICIO 1 
print("Ejercicio 1: Crear al menos 3 tablas con tres atributos cada una")
# crear_tablas(dynamodb)

# Ejercicio 2: Crear tres registros encada tabla
# crear_registros(dynamodb)

# Ejercicio 3: Obtener un registro de cada tabla
# obtener_registros(dynamodb)

# Ejercicio 4: Actualizar un registro de cada tabla
# actualizar_registros()

# Ejercicio 5: Eliminar un registro de cada tabla 
# eliminar_registro_comentarios()
# eliminar_registro_cursos()
# eliminar_registro_persona()

# Ejercicio 6: Obtener todos los registros de cada tabla
# mostrar_registros('Comentario')
# mostrar_registros('Persona')
# mostrar_registros('Cursos')

# Ejercicio 7: Obtener una conjunto de registros de un filtrado de cada tabla
# filtrar_registros(dynamodb)

# Ejercicio 8: Realizar una eliminación condicional de cada tabla
# eliminar_registros_condicionalmente(dynamodb)

# Ejercicio 9: Obtener un conjunto de datos a través de varios filtros aplicado en cada tabla
filtrar_registros_complejos(dynamodb)

# Ejercicio 10: Utilizar PartiQL statement en cada tabla
ejecutar_partiql(dynamodb)

crear_backup(dynamodb)