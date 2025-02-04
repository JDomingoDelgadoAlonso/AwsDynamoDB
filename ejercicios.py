from conexion import obtener_conexion
import boto3
import json
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

dynamodb = obtener_conexion()  # Mantenerlo aquí es correcto
client = dynamodb.meta.client

def crear_tablas(dynamodb):
    client = dynamodb.meta.client
    # Crear tabla 1: Cursos con un índice secundario local (LSI)
    client.create_table(
        TableName='Cursos',
        KeySchema=[
            {
                'AttributeName': 'ID',
                'KeyType': 'HASH'  # Partition Key
            },
            {
                'AttributeName': 'Categoria',
                'KeyType': 'RANGE'  # Sort Key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'ID',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Categoria',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Duracion',
                'AttributeType': 'N'
            }
        ],
        LocalSecondaryIndexes=[
            {
                'IndexName': 'DuracionIndex',  # LSI para ordenar por Duracion
                'KeySchema': [
                    {
                        'AttributeName': 'ID',
                        'KeyType': 'HASH'  # Igual que la clave HASH de la tabla
                    },
                    {
                        'AttributeName': 'Duracion',
                        'KeyType': 'RANGE'  # Clave de ordenación adicional
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'  # Incluye todos los atributos
                }
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Crear tabla 2: Persona con un índice secundario local (LSI)
    client.create_table(
        TableName='Persona',
        KeySchema=[
            {
                'AttributeName': 'ID',
                'KeyType': 'HASH'  # Partition Key
            },
            {
                'AttributeName': 'Email',
                'KeyType': 'RANGE'  # Sort Key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'ID',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Email',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Edad',
                'AttributeType': 'N'
            }
        ],
        LocalSecondaryIndexes=[
            {
                'IndexName': 'EdadIndex',  # LSI para ordenar por Edad
                'KeySchema': [
                    {
                        'AttributeName': 'ID',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'Edad',
                        'KeyType': 'RANGE'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Crear tabla 3: Comentario con un índice secundario local (LSI)
    client.create_table(
        TableName='Comentario',
        KeySchema=[
            {
                'AttributeName': 'ID',
                'KeyType': 'HASH'  # Partition Key
            },
            {
                'AttributeName': 'LugarPost',
                'KeyType': 'RANGE'  # Sort Key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'ID',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'LugarPost',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Titulo',
                'AttributeType': 'S'
            }
        ],
        LocalSecondaryIndexes=[
            {
                'IndexName': 'TituloIndex',  # LSI para ordenar por Titulo
                'KeySchema': [
                    {
                        'AttributeName': 'ID',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'Titulo',
                        'KeyType': 'RANGE'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    tabla = dynamodb.Table('Comentario')
    tabla.wait_until_exists()
    print("Tablas creadas exitosamente con índices secundarios locales.")

# 2 - Crear tres registros encada tabla 
def crear_registros(dynamodb):
    # tablas y sus registros a insertar
    tablas_registros = [
        {
            'tabla': 'Comentario',
            'registros': [
                {'ID': '1', 'LugarPost': 'Lugar1', 'Titulo': 'Buen Curso, pero...', 'Contenido': 'En la mitad del curso el nivel baja mucho'},
                {'ID': '2', 'LugarPost': 'Lugar2', 'Titulo': 'Nada que añadir', 'Contenido': 'Perfe'},
                {'ID': '3', 'LugarPost': 'Lugar3', 'Titulo': 'Perfecto todo', 'Contenido': 'Perfecto, el tiempo se me ha pasado volando'}
            ]
        },
        {
            'tabla': 'Cursos',
            'registros': [
                {'ID': '1', 'Categoria': 'Desarrollo de Aplicaciones', 'Duracion': 10},
                {'ID': '2', 'Categoria': 'Ingles', 'Duracion': 20},
                {'ID': '3', 'Categoria': 'Diseño de Interiores', 'Duracion': 150}
            ]
        },
        {
            'tabla': 'Persona',
            'registros': [
                {'ID': '1', 'Nombre': 'Juan', 'Email': 'Juan@juan.com', 'Edad': 25},
                {'ID': '2', 'Nombre': 'Gabri',  'Email': 'gabri@gabri.com', 'Edad': 30},
                {'ID': '3', 'Nombre': 'Javi',  'Email': 'javi@javi.com', 'Edad': 22}
            ]
        }
    ]
    
    # Creamos los registros en cada tabla
    for tabla_info in tablas_registros:
        tabla_nombre = tabla_info['tabla']
        registros = tabla_info['registros']
        
        tabla = dynamodb.Table(tabla_nombre)
        
        for registro in registros:
            try:
                # Insertamos un registro en la tabla
                response = tabla.put_item(
                    Item=registro
                )
                
                # Verificamos si la operación fue exitosa
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    print(f"Item added successfully to {tabla_nombre}: {registro}")
                else:
                    print(f"Error adding item to {tabla_nombre}: {registro}")
            
            except Exception as e:
                # Capturamos cualquier error que ocurra
                print(f"An error occurred while adding item to {tabla_nombre}: {e}")

# Función para convertir objetos Decimal a float o int (se utiliza para algunas salidas porque daba error en los decimales)
def convertir_decimal(obj):
    if isinstance(obj, Decimal):
        # Si el valor es Decimal, lo convertimos a float (o int si es un número entero)
        return float(obj) if obj % 1 else int(obj)
    raise TypeError("Tipo no serializable")

# 3 - Obtener un registro de cada tabla (1 punto)
def obtener_registros(dynamodb):
    # Lista de tablas y las claves para obtener un registro
    tablas_claves = [
        {
            'tabla': 'Comentario',
            'clave': {'ID': '1', 'LugarPost' : 'Lugar1'}
        },
        {
            'tabla': 'Cursos',
            'clave': {'ID': '1', 'Categoria' : 'Desarrollo de Aplicaciones'}
        },
        {
            'tabla': 'Persona',
            'clave': {'ID': '1', 'Email' : 'Juan@juan.com'}
        }
    ]
    
    # Obtener un registro de cada tabla
    for tabla_info in tablas_claves:
        tabla_nombre = tabla_info['tabla']
        clave = tabla_info['clave']
        
        tabla = dynamodb.Table(tabla_nombre)
        
        try:
            # Obtener el registro usando la clave primaria
            response = tabla.get_item(
                Key=clave
            )
            
            # Verificar si el registro existe
            if 'Item' in response:
                print(f"Registro obtenido de {tabla_nombre}:")
                print(json.dumps(response['Item'], default=convertir_decimal, indent=4))
            else:
                print(f"No se encontró el registro en {tabla_nombre} con la clave {clave}")
        
        except Exception as e:
            # Capturar cualquier error que ocurra
            print(f"An error occurred while getting item from {tabla_nombre}: {e}")

# 4 - Actualizar un registro de cada tabla
def actualizar_registros():
    # Tabla de Comentarios
    tabla_comentarios = dynamodb.Table('Comentario')
    
    try:
        # Actualizar un registro en la tabla Comentario
        response = tabla_comentarios.update_item(
            Key={
                'ID': '1', 'LugarPost' : 'Lugar1',  # Clave primaria (ID) la que vamos a actualizar
            },
            UpdateExpression="set Titulo = :titulo, Contenido = :contenido",  # Actualizamos Titulo y Contenido
            ExpressionAttributeValues={
                ':titulo': 'Curso mejorado',  # Nuevo valor de Titulo
                ':contenido': 'El curso ha mejorado mucho en la parte intermedia'  # Nuevo valor de Contenido
            },
            ReturnValues="ALL_NEW"  # Devuelve el ítem actualizado
        )
        print("Registro actualizado en Comentario:", response['Attributes'])
    
    except Exception as e:
        print(f"Error al actualizar el registro en Comentario: {e}")
    
    # Tabla de Cursos
    tabla_cursos = dynamodb.Table('Cursos')
    
    try:
        # Actualizar un registro en la tabla Cursos
        response = tabla_cursos.update_item(
            Key={
                'ID': '1', 'Categoria' : 'Desarrollo de Aplicaciones',  # Clave primaria (ID) del que queremos actualizar
            },
            UpdateExpression="set Duracion = :duracion",  # Registros que queremos actualizar: Duracion
            ExpressionAttributeValues={
                ':duracion': 12  # Nuevo valor de Duracion
            },
            ReturnValues="ALL_NEW"  # Devuelve el ítem actualizado
        )
        print("Registro actualizado en Cursos:", response['Attributes'])
    
    except Exception as e:
        print(f"Error al actualizar el registro en Cursos: {e}")
    
    # Tabla de Personas
    tabla_personas = dynamodb.Table('Persona')
    
    try:
        # Actualizar un registro en la tabla Persona
        response = tabla_personas.update_item(
            Key={
                'ID': '1', 'Email' : 'Juan@juan.com',  # Clave primaria (ID)
            },
            UpdateExpression="set Nombre = :nombre, Edad = :edad",  # Actualizamos Nombre y Edad
            ExpressionAttributeValues={
                ':nombre': 'Juan Carlos',  # Nuevo valor de Nombre
                ':edad': 26  # Nuevo valor de Edad
            },
            ReturnValues="ALL_NEW"  # Devuelve el ítem actualizado
        )
        print("Registro actualizado en Persona:", response['Attributes'])
    
    except Exception as e:
        print(f"Error al actualizar el registro en Persona: {e}")

# 5 - Elminar un registro de cada tabla
def eliminar_registro_comentarios():
    table = dynamodb.Table('Comentario')
    response = table.delete_item(
        Key={
            'ID': '1',
            'LugarPost': 'Lugar1'
        }
    )
    print("Registro de Comentario eliminado:", json.dumps(response, indent=4))

# 5- Eliminar un registro de la tabla Cursos
def eliminar_registro_cursos():
    table = dynamodb.Table('Cursos')
    response = table.delete_item(
        Key={
            'ID': '3',
            'Categoria': 'Diseño de Interiores'
        }
    )
    print("Registro de Cursos eliminado:", json.dumps(response, indent=4))

# 5 - Eliminar un registro de la tabla Persona
def eliminar_registro_persona():
    table = dynamodb.Table('Persona')
    response = table.delete_item(
        Key={
            'ID': '2',
            'Email': 'gabri@gabri.com'
        }
    )
    print("Registro de Persona eliminado:", json.dumps(response, indent=4))

# 6 - Obtener todos los registros de cada tabla
def mostrar_registros(nombre_tabla):
    table = dynamodb.Table(nombre_tabla)
    
    # Realizamos el escaneo de la tabla
    response = table.scan()
    data = response['Items']
    
    # Continuar escaneando si hay más datos
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    
    # Mostrar los registros
    print(f"Registros de la tabla {nombre_tabla}:")
    for item in data:
        print(json.dumps(item, default=convertir_decimal, indent=4))


# 7 - Obtener una conjunto de registros de un filtrado de cada tabla (1 punto)
def filtrar_registros(dynamodb):
    # Filtrar comentarios donde LugarPost sea "Lugar2"
    table_comentarios = dynamodb.Table("Comentario")
    filtered_items = []
    response = table_comentarios.scan(
        FilterExpression=Attr("LugarPost").eq("Lugar2")
    )
    filtered_items.extend(response["Items"])

    while "LastEvaluatedKey" in response:
        response = table_comentarios.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        filtered_items.extend(response["Items"])

    print("\nComentarios en Lugar2:")
    for item in filtered_items:
        print(json.dumps(item, default=convertir_decimal, indent=4))

    # Filtrar personas con edad mayor a 25
    table_personas = dynamodb.Table("Persona")
    filtered_items = []
    response = table_personas.scan(
        FilterExpression=Attr("Edad").gt(25)
    )
    filtered_items.extend(response["Items"])

    while "LastEvaluatedKey" in response:
        response = table_personas.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        filtered_items.extend(response["Items"])

    print("\nPersonas mayores de 25 años:")
    for item in filtered_items:
        print(json.dumps(item, default=convertir_decimal, indent=4))

    # Filtrar cursos con duración mayor a 10 y menor a 100
    table_cursos = dynamodb.Table("Cursos")
    filtered_items = []
    response = table_cursos.scan(
        FilterExpression=Attr("Duracion").gt(10) & Attr("Duracion").lt(100)
    )
    filtered_items.extend(response["Items"])

    while "LastEvaluatedKey" in response:
        response = table_cursos.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        filtered_items.extend(response["Items"])

    print("\nCursos con duración entre 10 y 100 horas:")
    for item in filtered_items:
        print(json.dumps(item, default=convertir_decimal, indent=4))

# 8 - Realizar una eliminación condicional de cada tabla (1 punto)
def eliminar_registros_condicionalmente(dynamodb):
    # Eliminar un comentario si el título contiene la palabra "Nada"
    table_comentarios = dynamodb.Table("Comentario")
    response = table_comentarios.delete_item(
        Key={
            'ID': '2',
            'LugarPost': 'Lugar2'
        },
        ConditionExpression="contains(Titulo, :val)",
        ExpressionAttributeValues={
            ':val': 'Nada'
        }
    )
    print("\nEliminación condicional en Comentario:")
    print(json.dumps(response, indent=4))

    # Eliminar una persona si su edad es menor o igual a 22
    table_personas = dynamodb.Table("Persona")
    response = table_personas.delete_item(
        Key={
            'ID': '3',
            'Email': 'javi@javi.com'
        },
        ConditionExpression="Edad <= :edad",
        ExpressionAttributeValues={
            ':edad': Decimal(22)
        }
    )
    print("\nEliminación condicional en Persona:")
    print(json.dumps(response, indent=4))

    # Eliminar un curso si su duración es menor que 13
    table_cursos = dynamodb.Table("Cursos")
    response = table_cursos.delete_item(
        Key={
            'ID': '1',
            'Categoria': 'Desarrollo de Aplicaciones'
        },
        ConditionExpression="Duracion < :duracion",
        ExpressionAttributeValues={
            ':duracion': Decimal(13)
        }
    )
    print("\nEliminación condicional en Cursos:")
    print(json.dumps(response, indent=4))

# 9 - Obtener un conjunto de datos a través de varios filtros aplicado en cada tabla (1 punto)
def filtrar_registros_complejos(dynamodb):
    # Comentario: registros cuando ID es "3" y el Titulo contiene "Perfecto" o "todo"
    table_comentarios = dynamodb.Table("Comentario")
    filtered_comentarios = []
    response = table_comentarios.scan(
        FilterExpression=Attr("ID").eq("3") & (Attr("Titulo").contains("Perfecto") | Attr("Titulo").contains("todo"))
    )
    filtered_comentarios.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table_comentarios.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"],
            FilterExpression=Attr("ID").eq("3") & (Attr("Titulo").contains("Perfecto") | Attr("Titulo").contains("todo"))
        )
        filtered_comentarios.extend(response["Items"])
    
    print("\nComentarios con ID = 3 y Titulo que contiene 'Perfecto' o 'todo':")
    for item in filtered_comentarios:
        print(json.dumps(item, default=convertir_decimal, indent=4))
    
    # Persona: registros con Email que contenga "juan.com" y Nombre que contenga "Carlos"
    table_personas = dynamodb.Table("Persona")
    filtered_personas = []
    response = table_personas.scan(
        FilterExpression=Attr("Email").contains("juan.com") & Attr("Nombre").contains("Carlos")
    )
    filtered_personas.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table_personas.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"],
            FilterExpression=Attr("Email").contains("juan.com") & Attr("Nombre").contains("Carlos")
        )
        filtered_personas.extend(response["Items"])
    
    print("\nPersonas con Email que contiene 'juan.com' y Nombre que contiene 'Carlos':")
    for item in filtered_personas:
        print(json.dumps(item, default=convertir_decimal, indent=4))
    
    # Cursos: registros de categoría "Ingles" y Duracion igual a 20
    table_cursos = dynamodb.Table("Cursos")
    filtered_cursos = []
    response = table_cursos.scan(
        FilterExpression=Attr("Categoria").eq("Ingles") & Attr("Duracion").eq(Decimal(20))
    )
    filtered_cursos.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table_cursos.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"],
            FilterExpression=Attr("Categoria").eq("Ingles") & Attr("Duracion").eq(Decimal(20))
        )
        filtered_cursos.extend(response["Items"])
    
    print("\nCursos de categoría 'Ingles' y con duración igual a 20:")
    for item in filtered_cursos:
        print(json.dumps(item, default=convertir_decimal, indent=4))

# 10 - Utilizar PartiQL statement en cada tabla (1 punto)
def ejecutar_partiql(dynamodb):
    client = dynamodb.meta.client

    # Comentario: obtener el registro con ID "3" y Titulo que contenga "Perfecto" o "todo"
    statement_comentarios = (
        'SELECT * FROM "Comentario" WHERE id=? AND (contains(titulo, ?) OR contains(titulo, ?))'
    )
    parameters_comentarios = [
        {'S': '3'},
        {'S': 'Perfecto'},
        {'S': 'todo'}
    ]
    response_comentarios = client.execute_statement(
        Statement=statement_comentarios,
        Parameters=parameters_comentarios
    )
    print("Comentarios con ID = 3 y Titulo que contiene 'Perfecto' o 'todo':")
    for item in response_comentarios.get("Items", []):
        print(json.dumps(item, indent=4))

    # Persona: obtener el nombre del registro cuyo Email contenga "juan.com" y cuyo Nombre contenga "Carlos"
    statement_personas = (
        'SELECT nombre FROM "Persona" WHERE contains(email, ?) AND contains(nombre, ?)'
    )
    parameters_personas = [
        {'S': 'juan.com'},
        {'S': 'Carlos'}
    ]
    response_personas = client.execute_statement(
        Statement=statement_personas,
        Parameters=parameters_personas
    )
    print("\nNombre de personas con Email que contiene 'juan.com' y Nombre que contiene 'Carlos':")
    for item in response_personas.get("Items", []):
        print(json.dumps(item, indent=4))

    # Cursos: obtener el registro donde Categoria sea "Ingles" y Duracion igual a 20
    statement_cursos = 'SELECT * FROM "Cursos" WHERE categoria=? AND duracion=?'
    parameters_cursos = [
        {'S': 'Ingles'},
        {'N': '20'}
    ]
    response_cursos = client.execute_statement(
        Statement=statement_cursos,
        Parameters=parameters_cursos
    )
    print("\nCursos de categoría 'Ingles' y con duración igual a 20:")
    for item in response_cursos.get("Items", []):
        print(json.dumps(item, indent=4))

# 11 - Crear un backup de todas las tablas (0.5 puntos)
def crear_backup(dynamodb):
    client = dynamodb.meta.client
    response = client.create_backup(
        TableName='Comentario',
        BackupName='Comentario-Backup-01'
    )
    print(response)
    response = client.create_backup(
        TableName='Cursos',
        BackupName='Cursos-Backup-01'
    )
    print(response)
    response = client.create_backup(
        TableName='Persona',
        BackupName='Persona-Backup-01'
    )
    print(response)