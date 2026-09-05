# Informe

## Datos del estudiante

Estudiante: Nicolás Ángel Garófalo
Padrón: 100952

## Protocolo

El protocolo implementado es un protocolo binario que se encarga de enviar los datos de las apuestas al servidor y recibir los resultados de los sorteos. 

## Mecanismos de concurrencia utilizados

Para resolver la necesidad de gestionar cada cliente de forma concurrente se recurrió a crear un Thread para cada uno utilizando threading.Thread. la clase ClientHandler hereda de Thread e implementa el método run(). El client_handler sabe reconocer qué tipo de mensaje recibió en base a MessageType, representando la traducción del byte interno de los tipos de mensajes posibles, gestionados por el protocolo. 

Se detectó como sección crítica el archivo bets.csv el cual accede cada client_handler del servidor tanto para leer (load_bets) como escribir (store_bets). Es por esto que se decidió implementar un monitor que encapsulara el acceso al archivo. La concurrencia dentro del monitor está presente en los siguientes casos:
- Cuando se solicita persistir un batch de apuestas. Al solicitarle esto, el thread debe adquirir el lock de acceso al archivo, lo que hace que el thread se bloquee en caso de que otro ya lo haya adquirido.
- Cuando se solicita leer los ganadores para un agency_id concreto, también se debe adquirir el lock.

Gracias al lock del archivo, la implementación es compatible con el escenario en el que ya se haya alcanzado quorum y hayan N agencias queriendo leer sus ganadores, a la vez que pueden haber otras M agencias queriendo todavía persistir sus apuestas.

Para implementar el quórum, se decidió utilizar una condition variable. La misma chequea 

Falta mencionar:
- como se hizo el corte del lock para el shutdown graceful
- quorum con condvar
- protocolo
- ver si vale la pena sección para explicar graceful shutdown
