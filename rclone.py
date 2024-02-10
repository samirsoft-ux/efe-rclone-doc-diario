import subprocess
from datetime import datetime
import zoneinfo
import ibm_boto3
from ibm_botocore.client import Config
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_secrets_manager_sdk.secrets_manager_v2 import SecretsManagerV2
import json

# Establecer la zona horaria de Lima, Perú
timezone_lima = zoneinfo.ZoneInfo("America/Lima")

secret_api_key = os.environ.get("SECRET_IBM_API_KEY")

# Configuración de IBM Secret Manager
authenticator = IAMAuthenticator(secret_api_key)
secrets_manager = SecretsManagerV2(authenticator=authenticator)
secrets_manager.set_service_url('https://65e7ac31-7d3d-4c5f-9545-f848e11f8a26.private.us-south.secrets-manager.appdomain.cloud')

def obtener_secreto(secret_id):
    response = secrets_manager.get_secret(id=secret_id)
    secret_data = response.get_result()
    # Corrección basada en la nueva comprensión de la estructura de los datos
    if 'data' in secret_data:
        secret_values = secret_data['data']
        return secret_values
    else:
        print("La estructura del secreto no es como se esperaba.")
        return {}

secret_id = os.environ.get("SECRET_ID_PORTAL")
secretos = obtener_secreto(secret_id)

def generar_nombre_bucket():
    fecha_actual = datetime.now(timezone_lima).strftime('%Y-%m-%d')
    
    # Generar una letra aleatoria basada en la hora actual
    letra_aleatoria = generar_letra_aleatoria()
    
    return f"backup-{fecha_actual}-{letra_aleatoria}"

def generar_letra_aleatoria():
    # Obtener el segundo actual
    segundo_actual = datetime.now().second
    
    # Convertir el segundo en un índice de letra (0=a, 1=b, ..., 25=z)
    indice_letra = segundo_actual % 26
    
    # Convertir el índice de letra en la letra correspondiente
    letra = chr(indice_letra + 97)
    
    return letra

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
    # Asegurarse de que todos los valores necesarios están presentes
    if all(key in secretos for key in ['SOURCE_ACCESS_KEY_ID', 'SOURCE_SECRET_ACCESS_KEY', 'DESTINATION_ACCESS_KEY_ID', 'DESTINATION_SECRET_ACCESS_KEY', 'SOURCE_ENDPOINT', 'DESTINATION_ENDPOINT']):
        print("Iniciando la creación de la configuración de rclone...")
        config = f"""
        [COS_SOURCE]
        type = s3
        provider = IBMCOS
        env_auth = false
        access_key_id = {secretos['SOURCE_ACCESS_KEY_ID']}
        secret_access_key = {secretos['SOURCE_SECRET_ACCESS_KEY']}
        endpoint = {secretos['SOURCE_ENDPOINT']}

        [COS_DESTINATION]
        type = s3
        provider = IBMCOS
        env_auth = false
        access_key_id = {secretos['DESTINATION_ACCESS_KEY_ID']}
        secret_access_key = {secretos['DESTINATION_SECRET_ACCESS_KEY']}
        endpoint = {secretos['DESTINATION_ENDPOINT']}
        """
        with open("rclone.conf", "w") as file:
            file.write(config)
        print("Configuración de rclone creada exitosamente.")
    else:
        print("Error: No se encontraron las claves esperadas en los secretos. Revisa la recuperación del secreto.")

def aplicar_politica_ciclo_vida(bucket_name):
    cos_client = ibm_boto3.client('s3',
                                  ibm_api_key_id=secretos['IBM_COS_API_KEY'],
                                  ibm_service_instance_id=secretos['IBM_SERVICE_INSTANCE_ID'],
                                  config=Config(signature_version='oauth'),
                                  endpoint_url=secretos['IBM_COS_ENDPOINT'])

    politica_ciclo_vida = {
        'Rules': [
            {
                'ID': 'ArchiveAllObjects',
                'Status': 'Enabled',
                'Filter': {},
                'Transitions': [
                    {
                        'Days': int(secretos['DIAS_PARA_ARCHIVAR']),
                        'StorageClass': 'GLACIER'
                    }
                ]
            },
            {
                'ID': 'DeleteAllObjects',
                'Status': 'Enabled',
                'Filter': {},
                'Expiration': {
                    'Days': int(secretos['DIAS_PARA_ELIMINAR'])
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

crear_configuracion_rclone()
nombre_bucket_fecha = generar_nombre_bucket()
crear_bucket_con_rclone(nombre_bucket_fecha)
aplicar_politica_ciclo_vida(nombre_bucket_fecha)

# Continuación del script...
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
