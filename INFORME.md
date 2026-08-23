
## Protocolo
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