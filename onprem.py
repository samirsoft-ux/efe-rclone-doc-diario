from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_secrets_manager_sdk.secrets_manager_v2 import SecretsManagerV2

# Configura tus credenciales y detalles de IBM Secrets Manager aquí
IBM_CLOUD_API_KEY = 'dJxj9q28QtO_SvoAk6guuC1kqOE5UfwfXWCaP6FwbRII'
SECRET_MANAGER_URL = 'https://65e7ac31-7d3d-4c5f-9545-f848e11f8a26.us-south.secrets-manager.appdomain.cloud'
SECRET_ID = 'e4d3d765-6255-f517-cd2e-b76551c9b56c'

def main():
    # Autenticador con tu API key de IBM Cloud
    authenticator = IAMAuthenticator(IBM_CLOUD_API_KEY)
    
    # Instancia del servicio de Secrets Manager
    secrets_manager = SecretsManagerV2(authenticator=authenticator)
    secrets_manager.set_service_url(SECRET_MANAGER_URL)
    
    try:
        # Recuperar el secreto usando su ID
        response = secrets_manager.get_secret(id=SECRET_ID)
        secret_data = response.get_result()
        
        # Accede directamente a la clave 'data' para los secretos tipo clave-valor
        if 'data' in secret_data:
            kv_data = secret_data['data']
            print("Datos del secreto recuperados:", kv_data)
        else:
            print("El secreto no tiene el formato esperado o está vacío.")
    except Exception as e:
        print("Ocurrió un error al recuperar el secreto:", e)

if __name__ == "__main__":
    main()