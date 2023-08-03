from __future__ import print_function

import os.path
import tempfile
import io

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Scope (alcanze del permiso), para modificarlo eliminar el archivo token.json
SCOPES = ['https://www.googleapis.com/auth/drive']

def main():
    
    #Variable creds, donde se guardara la credencial
    creds = None
    
    #Valida la credencial con el token
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    #Si el token no existe, se crea accediendo desde el navegador a la cuenta de drive y brindando el permiso
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        #Se guarda la credencial en el token para futuras ejecuciones
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('drive', 'v3', credentials=creds)
    file_id = ["1vS3z7-_St38mSgRgovdhucZlhswZW6jh", "1AF9yKBUeGjLVXc43UdP7Q60ZvDz8cl2J"]#Aca se colocan los ID de archivo

    for i in range(0, len(file_id)):

        #Obtener la información del archivo
        file = service.files().get(fileId=file_id[i]).execute()
        file_name = file['name']

        #Descargar el archivo
        request = service.files().get_media(fileId=file_id[i])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False

        while done is False:
            status, done = downloader.next_chunk()
            print("Descargando {}...".format(file_name))

        #Guardar el archivo en una ubicación temporal
        file_path = os.path.join(tempfile.gettempdir(), file_name)
        with open(file_path, 'wb') as f:
            fh.seek(0)
            f.write(fh.read())

        print("Archivo descargado y guardado temporalmente en: {}".format(file_path))

 #if __name__ == '__main__':
main()