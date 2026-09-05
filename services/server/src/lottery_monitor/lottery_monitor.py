from lottery import Lottery, Bet
from threading import Condition, Lock

class LotteryMonitor:

    def __init__(self, bets_file_path: str, agency_quorum_min: int):
        self.file_lock = Lock()
        self.condition = Condition()
        self.lottery = Lottery(bets_file_path)
        self.agency_quorum_min = agency_quorum_min
        self.agencies_waiting = 0
        self.quorum_reached = False
        self.stop_requested = False
    
    def store_bets(self, bets: list[Bet]):
        with self.file_lock:
            self.lottery.store_bets(bets)
    
    def get_winners(self, agency_id: int):
        with self.condition:
            self.agencies_waiting += 1
            
            if self.agencies_waiting == self.agency_quorum_min:
                self.quorum_reached = True
                self.condition.notify_all()
            else:
                while not self.quorum_reached and not self.stop_requested:
                    self.condition.wait()

            if self.stop_requested:
                return
                
        with self.file_lock:
            for bet in self.lottery.load_bets():
                if bet.agency_id == agency_id and self.lottery.has_won(bet):
                    yield bet
    
    def stop(self):
        with self.condition:
            self.stop_requested = True
            self.condition.notify_all()
            
        