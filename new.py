import subprocess
from datetime import datetime
import zoneinfo
import ibm_boto3
from ibm_botocore.client import Config
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_secrets_manager_sdk import SecretsManagerV1

# Establecer la zona horaria de Lima, Perú
timezone_lima = zoneinfo.ZoneInfo("America/Lima")

# Configuración de IBM Secret Manager
authenticator = IAMAuthenticator('dJxj9q28QtO_SvoAk6guuC1kqOE5UfwfXWCaP6FwbRII')
secrets_manager = SecretsManagerV1(authenticator=authenticator)
secrets_manager.set_service_url('https://65e7ac31-7d3d-4c5f-9545-f848e11f8a26.private.us-south.secrets-manager.appdomain.cloud')
secret_id = 'e4d3d765-6255-f517-cd2e-b76551c9b56c'

def obtener_secreto():
    response = secrets_manager.get_secret(secret_id)
    secret_data = response.get_result()
    # Asumiendo que el secreto es un objeto JSON con tus variables
    secret_values = secret_data['resources'][0]['secret_data']['payload']
    return secret_values

# Llama a la función una vez y almacena los valores para su uso posterior en el script
secretos = obtener_secreto()

def generar_nombre_bucket():
    fecha_actual = datetime.now(timezone_lima)
    return f"backup-{fecha_actual.strftime('%Y-%m-%d')}"

def crear_bucket_con_rclone(bucket_name):
    comando = f"rclone mkdir COS_DESTINATION:{bucket_name} --config rclone.conf"
    print(f"Creando bucket: {bucket_name}")
    stdout, stderr = ejecutar_comando_rclone(comando)
    if stderr:
        print(f"No se pudo crear el bucket {bucket_name}: {stderr}")

def ejecutar_comando_rclone(comando):
    print(f"Ejecutando comando: {comando}")
    proceso = subprocess.Popen(comando, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proceso.communicate()
    if stdout:
        print("Salida del comando:", stdout.decode())
    if stderr:
        print("Errores del comando:", stderr.decode())
    return stdout.decode(), stderr.decode()

def crear_configuracion_rclone():
    print("Iniciando la creación de la configuración de rclone...")
    # Los valores ahora se recuperan del diccionario 'secretos'
    source_access_key_id = secretos['SOURCE_ACCESS_KEY_ID']
    source_secret_access_key = secretos['SOURCE_SECRET_ACCESS_KEY']
    destination_access_key_id = secretos['DESTINATION_ACCESS_KEY_ID']
    destination_secret_access_key = secretos['DESTINATION_SECRET_ACCESS_KEY']
    source_endpoint = secretos['SOURCE_ENDPOINT']
    destination_endpoint = secretos['DESTINATION_ENDPOINT']
    config = f"""
    [COS_SOURCE]
    type = s3
    provider = IBMCOS
    env_auth = false
    access_key_id = {source_access_key_id}
    secret_access_key = {source_secret_access_key}
    endpoint = {source_endpoint}

    [COS_DESTINATION]
    type = s3
    provider = IBMCOS
    env_auth = false
    access_key_id = {destination_access_key_id}
    secret_access_key = {destination_secret_access_key}
    endpoint = {destination_endpoint}
    """
    with open("rclone.conf", "w") as file:
        file.write(config)
    print("Configuración de rclone creada exitosamente.")

def aplicar_politica_ciclo_vida(bucket_name):
    # Los valores ahora se recuperan del diccionario 'secretos'
    cos_client = ibm_boto3.client('s3',
                                  ibm_api_key_id=secretos['IBM_COS_API_KEY'],
                                  ibm_service_instance_id=secretos['IBM_SERVICE_INSTANCE_ID'],
                                  config=Config(signature_version='oauth'),
                                  endpoint_url=secretos['IBM_COS_ENDPOINT'])

    dias_para_archivar = int(secretos['DIAS_PARA_ARCHIVAR'])
    dias_para_eliminar = int(secretos['DIAS_PARA_ELIMINAR'])

    politica_ciclo_vida = {
        'Rules': [
            {
                'ID': 'ArchiveAllObjects',
                'Status': 'Enabled',
                'Filter': {},  # Aplica la regla a todos los objetos
                'Transitions': [
                    {
                        'Days': dias_para_archivar,
                        'StorageClass': 'GLACIER'
                    }
                ]
            },
            {
                'ID': 'DeleteAllObjects',
                'Status': 'Enabled',
                'Filter': {},  # Aplica la regla a todos los objetos
                'Expiration': {
                    'Days': dias_para_eliminar
                }
            }
        ]
    }

    try:
        cos_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=politica_ciclo_vida
        )
        print(f"Política de ciclo de vida aplicada al bucket: {bucket_name}.")
    except cos_client.exceptions.ClientError as e:
        print(f"Error al aplicar la política de ciclo de vida: {e}")

# Llama a las funciones en el orden necesario
crear_configuracion_rclone()
nombre_bucket_fecha = generar_nombre_bucket()
crear_bucket_con_rclone(nombre_bucket_fecha)
aplicar_politica_ciclo_vida(nombre_bucket_fecha)

# Variables y configuración de rclone para la transferencia (omitidas por brevedad, ya proporcionadas)
checkers = 64  # Número de hilos de verificación en paralelo
transfers = 128  # Número de objetos a transferir en paralelo
multi_thread_streams = 4  # Descarga de archivos grandes en partes en paralelo
s3_upload_concurrency = 4  # Número de partes de archivos grandes a subir en paralelo

cos_source_bucket = secretos['COS_SOURCE_NAME']
cos_destination_bucket = nombre_bucket_fecha

print("Verificando la configuración del bucket de origen...")
ejecutar_comando_rclone(f"rclone lsd COS_SOURCE:{cos_source_bucket} --config rclone.conf")

print("Verificando la configuración del bucket de destino...")
ejecutar_comando_rclone(f"rclone lsd COS_DESTINATION:{cos_destination_bucket} --config rclone.conf")

# Dry run y copia real (omitidos por brevedad, ya proporcionados)
# Comando de rclone con flags para la prueba (dry run)
comando_dry_run = (
    f"rclone --dry-run copy COS_SOURCE:{cos_source_bucket} COS_DESTINATION:{cos_destination_bucket} "
    f"--checkers {checkers} "
    f"--transfers {transfers} "
    f"--multi-thread-streams {multi_thread_streams} "
    f"--s3-upload-concurrency {s3_upload_concurrency} "
    f"-vv --config rclone.conf"
)
print("Iniciando dry run de rclone...")
stdout, stderr = ejecutar_comando_rclone(comando_dry_run)

# Comando de rclone con flags para la copia real
comando_copia = (
    f"rclone copy COS_SOURCE:{cos_source_bucket} COS_DESTINATION:{cos_destination_bucket} "
    f"--checkers {checkers} "
    f"--transfers {transfers} "
    f"--multi-thread-streams {multi_thread_streams} "
    f"--s3-upload-concurrency {s3_upload_concurrency} "
    f"-vv --checksum --config rclone.conf"
)
print("Iniciando copia real de rclone...")
stdout, stderr = ejecutar_comando_rclone(comando_copia)
