from lottery import Lottery, Bet
from threading import Condition, Lock

class LotteryMonitor:
    """
    Clase que gestiona el acceso a la sección crítica (el archivo de apuestas y el contador de quorum).
    Cuenta con una condvar para la gestión del quorum y notificar a los hilos cuando se alcanza la meta.
    Cuenta con un file_lock para escritura y lectura atómica del archivo de apuestas.
    """

    def __init__(self, bets_file_path: str, agency_quorum_min: int):
        self.file_lock = Lock()
        self.condition = Condition()
        self.lottery = Lottery(bets_file_path)
        self.agency_quorum_min = agency_quorum_min
        self.agencies_waiting = 0
        self.quorum_reached = False
        self.stop_requested = False
    
    def store_bets(self, bets):
        """
        Método bloqueante. Recibe las apuestas. 
        Obtiene el lock. Si se solicitó detener el servidor, lo libera inmediatamente sin escribir. 
        Si no, almacena las apuestas en el archivo bets.csv y libera el lock.
        """
        with self.file_lock:
            if self.stop_requested:
                return
            self.lottery.store_bets(bets)
    
    def get_winners(self, agency_id: int):
        """
        Método bloqueante. Recibe el agency_id y obtiene los ganadores.
        Obtiene el lock de la condvar y corrobora la condición de quorum.
        Si es la última agencia en esperar, establece como quorum alcanzado y notifica a los hilos dormidos
        Si no es el caso, se duerme hasta que se alcance el quorum.
        Una vez notificada, se obtiene el lock del archivo para poder leerlo y obtener los ganadores.
        En caso de que se haya solicitado detener el servidor, no lee el archivo y retorna.
        """
        with self.condition:
            self.agencies_waiting += 1
            
            if self.agencies_waiting == self.agency_quorum_min:
                self.quorum_reached = True
                self.condition.notify_all()
            else:
                while not self.quorum_reached and not self.stop_requested:
                    self.condition.wait()

            if self.stop_requested: # En caso de que se haya despertado porque hay que detenerse, retorna
                return
                
        with self.file_lock:
            if self.stop_requested: # En caso de que haya adquirido el lock porque debe detenerse, retorna
                return
            for bet in self.lottery.load_bets():
                if bet.agency_id == agency_id and self.lottery.has_won(bet):
                    # Devuelve las apuestas de forma lazy (para respetar el yield de lottery)
                    # Esto permite no cargar todos los ganadores en memoria, y enviar por socket un ganador a la vez.
                    yield bet 
    
    def stop(self):
        """
        Método bloqueante.
        Establece stop_requested en True
        Despierta a todos los hilos bloqueados en el monitor
        """
        with self.condition:
            self.stop_requested = True
            self.condition.notify_all()
            
        