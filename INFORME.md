# Informe

## Datos del estudiante

Estudiante: Nicolás Ángel Garófalo
Padrón: 100952

## Protocolo implementado

El protocolo implementado consiste en enviar mensajes con el siguiente formato:

MSG_CODE | MSG_SIZE | MESSAGE

- MSG_CODE: Código de un byte que identifica el tipo de mensaje. Los códigos posibles son:
    - Handshake Code (0x01): Codigo que representa el primer mensaje que envía el cliente, su mensaje contendrá el agency_id. 
    - Batch Bet Code (0x02): Representa el envío de una apuesta en el MESSAGE.
    - Batch ACK Code (0x03): Representa la confirmación de parte del servidor, luego de procesar un lote de apuestas.
    - Winner Code (0x04): Representa que el mensaje contendrá un ganador en el MESSAGE.
    - End Bets Code (0x05): Representa el envío del fin de las apuestas por parte del cliente, luego de enviar todos sus lotes.
    - End Winners Code (0x06): Representa el envío del fin de los ganadores por parte del servidor.
- MSG_SIZE: Tamaño del MESSAGE representado como un numero que ocupa 2 bytes en big-endian.
- MESSAGE: Contenido del mensaje.

Los mensajes con código Batch ACK Code, End Bets Code y End Winners Code no tienen MESSAGE ni MSG_SIZE. El mensaje como tal es únicamente el código ya que el mismo comunica el fin de determinada etapa en el flujo de comunicación entre cliente y servidor.

La idea principal es que el client_handler sepa que tipo de mensaje recibió para poder saber cómo procesarla. La separación entre códigos internos y los MessageType está para separar la responsabilidad. Los internos los interpreta el protocolo, y los transforma en MessageType para que el servidor los pueda comprender.

## Mecanismos de concurrencia utilizados

Para resolver la necesidad de gestionar cada cliente de forma concurrente se recurrió a crear un Thread para cada uno utilizando threading.Thread. la clase ClientHandler hereda de Thread e implementa el método run(). El client_handler sabe reconocer qué tipo de mensaje recibió en base a MessageType, representando la traducción del byte interno de los tipos de mensajes posibles, gestionados por el protocolo. 

Se detectó como sección crítica el archivo bets.csv el cual accede cada client_handler del servidor tanto para leer (load_bets) como escribir (store_bets). Es por esto que se decidió implementar un monitor que encapsulara el acceso al archivo. La concurrencia dentro del monitor está presente en los siguientes casos:
- Cuando se solicita persistir un batch de apuestas. Al solicitarle esto, el thread debe adquirir el lock de acceso al archivo, lo que hace que el thread se bloquee en caso de que otro ya lo haya adquirido.
- Cuando se solicita leer los ganadores para un agency_id concreto, también se debe adquirir el lock.

Gracias al lock del archivo, la implementación es compatible con el escenario en el que ya se haya alcanzado quorum y hayan N agencias queriendo leer sus ganadores, a la vez que pueden haber otras M agencias queriendo todavía persistir sus apuestas.

### Implementación del Quorum

Para implementar el quorum, se decidió utilizar una condition variable. Dentro de esta sección crítica se tiene un contador (protegido por la condvar) que sube por cada agencia que está esperando. Si no se cumple el quorum, el hilo del client_handler cede el lock interno de la condvar y se duerme. Si justo entra la agencia que alcanza el quorum, se realiza un notify_all() y por ende se despiertan los otros hilos. Vale la pena recordar que aunque todos los hilos se despierten para leer a sus ganadores, la lectura de los mismos está protegida a través del file_lock mencionado previamente.

## Casos relevantes del graceful shutdown

Cabe mencionar que cuando el programa recibe el signal SIGTERM, puede ocurrir que haya client_handlers dormidos por la condvar. En caso de que esto ocurra, el stop() de lottery_monitor (invocado por el server) despierta a los threads a través del notify_all() para que puedan finalizar sus ejecuciones.

Para el caso de los threads dormidos por el file_lock, si bien no hay una manera de despertarlos cuando reciben un SIGTERM, la escritura y lectura protegida por el lock son acotadas en tiempo (a diferencia del .wait() de la condvar, que podría ser indefinido si nunca se alcanzara el quorum). Por lo tanto, apenas el hilo que tiene el lock finalice con su uso y lo libere, cada uno de los hilos irán adquiriendo e inmediatamente liberandolo porque la condición de stop_requested es True (la cual se valida inmediatamente luego de obtener el lock).

Se podría hilar aún más fino en una implementación donde en vez de gestionar esto con un lock, se haga con una condvar para poder aprovechar el notify_all al recibir un SIGTERM, pero se debería comparar la diferencia de performance entre ambas implementaciones para decidir si justifica la implementación de otra condvar contra un lock simple.
