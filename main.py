from conexion import obtener_conexion
from ejercicios import*

# Obtener el cliente de DynamoDB
dynamodb = obtener_conexion()

print("===============================================================================================\n")
print("Ejercicio 1: Crear al menos 3 tablas con tres atributos cada una")
crear_tablas(dynamodb)

print("===============================================================================================\n")
print("Ejercicio 2: Crear tres registros encada tabla")
crear_registros(dynamodb)

print("===============================================================================================\n")
print("Ejercicio 3: Obtener un registro de cada tabla")
obtener_registros(dynamodb)

print("===============================================================================================\n")
print("Ejercicio 4: Actualizar un registro de cada tabla")
actualizar_registros()

print("===============================================================================================\n")
print("Ejercicio 5: Eliminar un registro de cada tabla") 
eliminar_registro_comentarios()
eliminar_registro_cursos()
eliminar_registro_persona()

print("===============================================================================================")
print("Ejercicio 6: Obtener todos los registros de cada tabla")
mostrar_registros('Comentario')
mostrar_registros('Persona')
mostrar_registros('Cursos')

print("===============================================================================================\n")
print("Ejercicio 7: Obtener una conjunto de registros de un filtrado de cada tabla")
filtrar_registros(dynamodb)

print("===============================================================================================\n")
print("Ejercicio 8: Realizar una eliminación condicional de cada tabla")
eliminar_registros_condicionalmente(dynamodb)

print("===============================================================================================\n")
print("Ejercicio 9: Obtener un conjunto de datos a través de varios filtros aplicado en cada tabla")
filtrar_registros_complejos(dynamodb)

print("===============================================================================================\n")
print("Ejercicio 10: Utilizar PartiQL statement en cada tabla")
ejecutar_partiql(dynamodb)

print("===============================================================================================\n")
print("Ejercicio 11: Creamos el backup para cada tabla")
crear_backup(dynamodb)
print("===============================================================================================\n")