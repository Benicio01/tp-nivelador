
## Protocolo (Ej 4)

Para el desarrollo del Ej 4 se optó por crear un protocolo con el siguiente formato
```
+--------------------------------+----------------------+
| 2 bytes: longitud N            | N bytes de payload   |
| (big-endian)                   |                      |
+--------------------------------+----------------------+
```

El receptor sabe de antemano exactamente cuánto esperar, el payload puede
contener cualquier byte, y el servidor responde reenviando el frame completo,
que ya es un mensaje válido por sí mismo. Los `\n` quedan fuera del protocolo:
son formato del CSV y se agregan únicamente al persistir en `OUTPUT_FILE`.

Definiciones menores: 2 bytes alcanzan para 64 KiB por mensaje, suficiente
para una apuesta y con margen para los lotes del ejercicio 6 sin tocar el
transporte; se usa big-endian, codificado con
`encoding/binary` en Go e `int.from_bytes(h, "big")` en Python.

La sincronización queda dada íntegramente por el intercambio de mensajes —
el cliente espera el eco de cada apuesta antes de enviar la siguiente, y el
servidor detecta el fin de sesión por cierre limpio— sin esperas temporales.


## Protocolo (Ej 5)

Los mensajes siguen utilizando el mismo header de antes, pero se agregan cambios en el cliente y servidor para cumplir con el funcionamiento esperado

### Serialización de los datos

El payload de una apuesta es una fila CSV de 6 columnas:
`agency_id,first_name,last_name,document,birthdate,number`.
El `agency_id` no está en `INPUT_FILE` (sale de la env `AGENCY_ID`) y se
antepone a los 5 campos de cada registro. Los ganadores se transmiten con
el mismo formato. Los `\n` quedan fuera del protocolo: son formato del CSV
y se agregan al persistir en `OUTPUT_FILE`, que guarda solo las 5 columnas
de cada ganador (sin `agency_id`).

### Intercambio de mensajes

1. El cliente envía una apuesta por mensaje, sin esperar respuesta por cada
   una.
2. Al terminar, cierra su lado de escritura del socket. El
   servidor interpreta ese EOF como "fin de apuestas de esa agencia".
3. El servidor persiste las apuestas recibidas y
   calcula los ganadores, filtrando por la agencia de la conexión.
4. Responde un mensaje por ganador y cierra la conexión; el cliente termina
   la lectura al detectar el cierre.

### Separación de responsabilidades

- transporte: `safe_socket` (envío/lectura completa);
- protocolo: serialización/deserialización de apuestas y armado de frames;
- dominio: `Bet` y `Lottery` (`src_frozen`, sin modificar);
- orquestación: `client.go`/`server.py`
