
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
para una apuesta y con margen para los batchs del ejercicio 6 sin tocar el
transporte. Se usa big-endian y el header se serializa y deserializa a mano (sin librerías) mediante shifts en Go y usando los metodos `from_bytes()` y `to_bytes()` de Python.

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


## Protocolo (Ej 6)

Se agrega procesamiento por batchs: el cliente agrupa `BATCH_SIZE`
apuestas en un único mensaje en lugar de enviarlas una por una.

### Formato de los mensajes

El header de 2 bytes se mantiene intacto, pero el payload de un frame puede
contener ahora varias apuestas. Cada apuesta se serializa igual que en el Ej 5
(fila CSV de 6 columnas), y las apuestas de un mismo batch se concatenan
separadas por `\n`:

El `\n` funciona como separador entre apuestas del batch porque los campos del
CSV no contienen comas ni saltos de línea. El `batch` viaja dentro de
un solo frame con longitud explícita, por lo que `send_all`/`recv_all` del
Ej 4 garantizan que se transfiere entero o se detecta el truncamiento — el
batch se procesa "todo o nada".

### Confirmación por batch (ACK)

Cambia la sincronización respecto del Ej 5 (donde el cliente enviaba todas las
apuestas y luego esperaba). Ahora el intercambio es request/response por batch:

1. El cliente envía un batch.
2. El servidor lee el frame completo, procesa **todas** las apuestas del batch
   y las persiste con `store_bets`.
3. Si el batch se procesó correctamente, responde un ACK (`OK`).
4. El cliente, tras recibir el ACK, recién envía el siguiente batch.
5. Al terminar los batches, el cliente envía el **batch residual** (las apuestas
   que no alcanzaron a completar un batch) y cierra su lado de escritura.
6. El servidor calcula los ganadores y responde **un mensaje por ganador**
   y el cliente los persiste en `OUTPUT_FILE` .


## Concurrencia (Ej 7)

El servidor atiende cada conexión en su propio thread, de modo que las agencias transmiten y persisten
sus batches en paralelo.

### Manejo de quorum

Al terminar de enviar los batches (y recibir todos los ACK), cada agencia
incrementa `_arrived_agencies` bajo la condición. Si con esa llegada se alcanza
`AGENCY_QUORUM_MIN`, se marca `_quorum_reached` y se hace `notify_all`: la agencia
que completó el mínimo pasa directo y despierta a las que estaban esperando en
el `wait()`; las que llegan después del flag también proceden directo.
(anteriormente se tenia una version con barrier por no entender bien la consigna del ejercicio)

### Server — estado compartido y sincronización

- `_lottery_lock`: protege el acceso al almacenamiento (`store_bets` / `load_bets`).
  Los workers escriben y leen la tabla de apuestas solo bajo este lock.
- `_quorum_cond` (`threading.Condition`): coordina la espera del quorum. Contar
  llegadas, marcar `_quorum_reached` y avisar con `notify_all` se hace bajo la misma
  condición.
- `_client_sockets_lock`: protege el `set` de sockets activos, tanto al agregar como
  al limpiar. `_close_client_sockets()` toma una copia del set con el lock y cierra
  los sockets después de soltarlo, así no sostiene el lock durante los cierres.
- `_shutdown` (`threading.Event`): flag que setea el handler de SIGTERM. 
   Se usa en la condición de espera del quorum y en el loop de accept: 
   cada agencia, cuando se despierta vuelve a mirar cuál de las dos cosas pasó (llegó el quorum o pidieron terminar) y actúa en consecuencia.


### Client — sincronización

El cliente no usa locks porque no hay estado compartido que proteger: solo
hay una goroutine de trabajo (`client.Run`) y una de control (`main`), y se
coordinan con canales.

- `signalChan`: `signal.Notify` deja ahí la señal cuando llega.
- `runErrChan`: la goroutine deja ahí el resultado de `Run()`.
- `main` hace `select`: se queda esperando a que llegue **una** de las dos cosas
  (la señal o el resultado) y recién entonces sigue.
- Al salir por señal, `main` espera `<-runErrChan` antes de terminar: no sale
  hasta que `Run()` terminó de limpiar (equivalente a un `join` de threads del
  lado del server).
- El buffer de 1 en ambos canales sirve para que el que envía no se quede
  esperando si `main` todavía no leyó.

## Graceful Shutdown (Ej 8)

Tanto el servidor como el cliente terminan de forma ordenada al recibir `SIGTERM` (a traves de `docker compose down`).

**Servidor:** el hilo principal registra un handler para `SIGTERM` que solo prende un flag (`_shutdown`). El loop que acepta conexiones usa un `timeout` de 1 segundo (TIMEOUT_SECONDS) para poder revisar ese flag y dejar de aceptar. Cuando se activa, cierra todos los sockets de clientes que estaban conectados y despierta a los threads que estaban esperando el quorum. Esos threads revisan el flag y se van sin intentar calcular ni enviar ganadores. Así no quedan threads colgados y todos los sockets y archivos se cierran. No se usa `exit` forzado.

**Cliente:** hay dos partes coordinadas con canales: la tarea principal (leer el archivo, mandar batches y esperar ganadores) y la escucha de señales. Si llega `SIGTERM` mientras está mandando o esperando, cierra la conexión a propósito para desbloquear la lectura/escritura y espera a que la tarea principal termine de cerrar los archivos de entrada y salida antes de salir (como un `join`). Si no hay señal, termina normal cuando se cierra la conexión del lado del servidor.

En ambos casos el cierre es rápido y libera todos los recursos.