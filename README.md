> version
0.1.0

> https://github.com/FiloSottile/mkcert/releases
> mover archivo dentro del proyecto
> Rename-Item .\mkcert-v1.4.4-windows-amd64.exe mkcert.exe
> ./mkcert.exe localhost
> renovar certificado: ./mkcert.exe localhost
# Ejemplo de comando para que el equipo genere sus propios certs
> mkcert -cert-file certs/localhost.pem -key-file certs/localhost-key.pem localhost
