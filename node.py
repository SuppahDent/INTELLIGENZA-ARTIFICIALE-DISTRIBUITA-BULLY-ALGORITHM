#!/usr/bin/env python3
"""
============================================================================
  BULLY ALGORITHM - Elezione del Coordinatore in un Sistema Distribuito
  Progetto di Sistemi Distribuiti
============================================================================

  Ogni nodo (P1-P5) è un processo separato che comunica via socket TCP
  su localhost. L'algoritmo implementa l'elezione del coordinatore usando
  esclusivamente tre tipi di messaggio: ELECTION, ANSWER, COORDINATOR.

  Uso: python node.py <ID_NODO>
       dove ID_NODO è un intero da 1 a 5

  Esempio:
    Terminale 1: python node.py 1
    Terminale 2: python node.py 2
    Terminale 3: python node.py 3
    Terminale 4: python node.py 4
    Terminale 5: python node.py 5
============================================================================
"""

import socket
import threading
import sys
import json
import time

# CONFIGURAZIONE DEL SISTEMA
# Mappa ID nodo -> porta TCP su localhost
# Ogni nodo ascolta su una porta univoca per ricevere messaggi
NODES = {
    1: 6001,
    2: 6002,
    3: 6003,
    4: 6004,
    5: 6005
}

HOST = '127.0.0.1'  # Tutti i nodi su localhost
T_ANSWER = 3 # T: tempo massimo di attesa per ricevere ANSWER dopo invio di ELECTION
T_COORDINATOR = 6 # T': tempo massimo di attesa per ricevere COORDINATOR dopo aver ricevuto ANSWER
HEARTBEAT_INTERVAL = 5 # intervallo di heartbeat per verificare lo stato del coordinatore

class BullyNode:
    """
    Rappresenta un singolo nodo nel sistema distribuito.
    Ogni nodo può avviare un'elezione, rispondere a elezioni altrui,
    dichiararsi coordinatore o riconoscere un nuovo coordinatore.
    """

    def __init__(self, node_id):
        self.node_id = node_id
        self.port = NODES[node_id]
        self.coordinator = None       # ID del coordinatore attuale (None = sconosciuto)
        self.running = True           # Flag per terminazione del nodo
        self.election_in_progress = False # Flag che indica se un'elezione è già in corso (evita elezioni duplicate)
        self.received_answer = threading.Event()         # Event per segnalare la ricezione di un messaggio ANSWER
        self.received_coordinator = threading.Event() # Event per segnalare la ricezione di un messaggio COORDINATOR
        self.lock = threading.Lock() # Lock per accesso thread-safe alle variabili condivise
        self._monitor_active = False # Flag per evitare monitor duplicati
        self._election_cooldown = 0  # Timestamp: ignora elezioni prima di questo momento

    def log(self, message):
        """Stampa un messaggio di log con timestamp e identificativo del nodo."""
        timestamp = time.strftime("%H:%M:%S")
        print(f"  [{timestamp}] [P{self.node_id}] {message}", flush=True)

    def start(self):
        """
        Avvia il nodo:
        1. Avvia il server TCP per ricevere messaggi
        2. Mostra il menu interattivo per l'utente
        """
        # Il server TCP gira in un thread daemon (si chiude con il processo)
        server_thread = threading.Thread(target=self._tcp_server, daemon=True)
        server_thread.start()

        self.log(f"Nodo avviato sulla porta {self.port}")
        time.sleep(0.5)

        # Menu interattivo nel thread principale
        self._interactive_menu()

    def _tcp_server(self):
        """
        Server TCP che ascolta i messaggi in arrivo dagli altri nodi.
        Ogni connessione viene gestita in un thread separato per
        permettere la ricezione concorrente di più messaggi.
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, self.port))
        server.listen(10)
        # Timeout di 1s per controllare periodicamente se il nodo è ancora attivo
        server.settimeout(1.0)

        while self.running:
            try:
                conn, addr = server.accept()
                # Ogni messaggio ricevuto viene gestito in un thread separato
                threading.Thread(
                    target=self._handle_connection,
                    args=(conn,),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except OSError:
                break

        server.close()

    def _handle_connection(self, conn):
        """Legge e processa un singolo messaggio da una connessione TCP."""
        try:
            data = conn.recv(4096).decode('utf-8')
            if data:
                msg = json.loads(data)
                self._handle_message(msg)
        except Exception:
            pass
        finally:
            conn.close()

    def _handle_message(self, msg):
        """
        Gestisce i tre tipi di messaggio dell'algoritmo Bully:
        - ELECTION: un nodo con ID inferiore chiede se ci sono nodi superiori attivi
        - ANSWER: un nodo con ID superiore conferma di essere attivo
        - COORDINATOR: un nodo si è dichiarato coordinatore
        """
        msg_type = msg['type']
        sender = msg['sender']

        # ELEZIONE
        # Ricevuto da un nodo con ID inferiore.
        # Devo rispondere con ANSWER (sono attivo e ho ID maggiore)
        # e avviare la mia elezione se non è già in corso.
        if msg_type == 'ELECTION':
            self.log(f"Ricevuto ELECTION da P{sender}")

            # Invio ANSWER al mittente per comunicare che sono attivo
            self._send_message(sender, 'ANSWER')
            self.log(f"Inviato ANSWER a P{sender}")

            # Avvio la mia elezione (se non già in corso e non in cooldown)
            # Questo è il meccanismo del "bullo": il nodo con ID più alto
            # prende il controllo dell'elezione
            with self.lock:
                if not self.election_in_progress and time.time() >= self._election_cooldown:
                    threading.Thread(
                        target=self.start_election,
                        daemon=True
                    ).start()

        # RISPOSTA
        # Ricevuto da un nodo con ID superiore: significa che quel nodo
        # è attivo e gestirà l'elezione. Devo attendere il COORDINATOR.
        elif msg_type == 'ANSWER':
            self.log(f"Ricevuto ANSWER da P{sender}")
            self.received_answer.set()

        # COORDINATORE
        # Un nodo si è dichiarato coordinatore. Registro il nuovo
        # coordinatore e interrompo eventuali elezioni in corso.
        elif msg_type == 'COORDINATOR':
            self.log(f"Ricevuto COORDINATOR da P{sender}")
            self.log(f"P{sender} è il nuovo coordinatore!")
            with self.lock:
                self.coordinator = sender
                # Arresto dell'elezione in corso alla ricezione di COORDINATOR
                self.election_in_progress = False
                # Cooldown: ignora nuove elezioni per 2 secondi per evitare
                # elezioni duplicate causate da messaggi ELECTION in ritardo
                self._election_cooldown = time.time() + 2
            self.received_coordinator.set()

            # Avvio il monitoraggio del coordinatore per rilevare guasti
            self._start_monitor()


    def _send_message(self, target_id, msg_type):
        """
        Invia un messaggio a un nodo specifico tramite una nuova connessione TCP.
        Ritorna True se l'invio ha successo, False se il nodo non è raggiungibile.
        
        Ogni messaggio è un oggetto JSON con:
        - type: tipo di messaggio (ELECTION, ANSWER, COORDINATOR)
        - sender: ID del nodo mittente
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((HOST, NODES[target_id]))
            msg = json.dumps({
                'type': msg_type,
                'sender': self.node_id
            })
            s.sendall(msg.encode('utf-8'))
            s.close()
            return True
        except (ConnectionRefusedError, socket.timeout, OSError):
            # Il nodo di destinazione non è raggiungibile (possibile guasto)
            return False

    def start_election(self):
        """
        Avvia una nuova elezione secondo l'algoritmo del Bullo:
        
        1. Invio ELECTION a tutti i nodi con ID superiore al mio
        2. Se nessun nodo con ID superiore risponde (timeout T): mi dichiaro coordinatore
        3. Se ricevo almeno un ANSWER: attendo il messaggio COORDINATOR (timeout T')
        4. Se il timeout T' scade senza COORDINATOR: riavvio l'elezione
        """
        with self.lock:
            if self.election_in_progress:
                return  # Elezione già in corso, evito duplicati
            # Se un coordinatore è stato eletto molto di recente, non serve una nuova elezione
            if time.time() < self._election_cooldown:
                return
            self.election_in_progress = True

        self.log("_____________________________")
        self.log("       AVVIO ELEZIONE ")
        self.log("_____________________________")

        # Reset degli eventi per questa nuova elezione
        self.received_answer.clear()
        self.received_coordinator.clear()

        # Identifico i nodi con ID superiore al mio
        higher_nodes = [nid for nid in NODES if nid > self.node_id]

        # Caso speciale: nessun nodo con ID superiore esiste nel sistema
        if not higher_nodes:
            self.log("Nessun nodo con ID superiore nel sistema")
            self._declare_coordinator()
            return

        # Invio ELECTION a tutti i nodi con ID superiore
        any_reachable = False
        for nid in higher_nodes:
            success = self._send_message(nid, 'ELECTION')
            if success:
                self.log(f"Inviato ELECTION a P{nid}")
                any_reachable = True
            else:
                self.log(f"P{nid} non raggiungibile")

        # Se nessun nodo superiore è raggiungibile, mi dichiaro coordinatore
        if not any_reachable:
            self.log("Nessun nodo superiore raggiungibile")
            self._declare_coordinator()
            return

        self.log(f"Attesa ANSWER (timeout T = {T_ANSWER}s)...")
        got_answer = self.received_answer.wait(timeout=T_ANSWER)

        if not got_answer:
            # Timeout T scaduto: nessun nodo superiore ha risposto -> Mi dichiaro coordinatore
            self.log(f"Timeout T scaduto: nessun ANSWER ricevuto")
            self._declare_coordinator()
        else:
            # Ho ricevuto almeno un ANSWER: un nodo superiore è attivo -> Attendo che quel nodo invii COORDINATOR
            self.log(f"ANSWER ricevuto. Attesa COORDINATOR (timeout T' = {T_COORDINATOR}s)...")
            got_coord = self.received_coordinator.wait(timeout=T_COORDINATOR)

            if not got_coord:
                # Timeout T' scaduto senza ricevere COORDINATOR
                # → Il nodo superiore potrebbe essersi guastato durante l'elezione
                # → Riavvio l'elezione
                self.log(f"Timeout T' scaduto: nessun COORDINATOR ricevuto")
                self.log("Riavvio elezione...")
                with self.lock:
                    self.election_in_progress = False
                self.start_election()

    def _declare_coordinator(self):
        """
        Il nodo si dichiara coordinatore e invia COORDINATOR a tutti
        i nodi con ID inferiore per notificarli.
        """
        with self.lock:
            self.coordinator = self.node_id
            self.election_in_progress = False
            # Cooldown: evita che ELECTION in ritardo da altri nodi
            # facciano ripartire un'elezione subito dopo la proclamazione
            self._election_cooldown = time.time() + 2

        self.log("_______________________________________")
        self.log("    MI DICHIARO COORDINATORE          ")
        self.log("_______________________________________")

        # Invio COORDINATOR a tutti i nodi con ID inferiore
        lower_nodes = [nid for nid in NODES if nid < self.node_id]
        for nid in lower_nodes:
            success = self._send_message(nid, 'COORDINATOR')
            if success:
                self.log(f"Inviato COORDINATOR a P{nid}")
            else:
                self.log(f"P{nid} non raggiungibile per COORDINATOR")

    # ========================================================================
    # MONITORAGGIO COORDINATORE - Rilevamento guasti
    # ========================================================================

    def _start_monitor(self):
        """Avvia il thread di monitoraggio se non è già attivo."""
        with self.lock:
            if self._monitor_active:
                return
            self._monitor_active = True
        threading.Thread(target=self._monitor_coordinator, daemon=True).start()

    def _monitor_coordinator(self):
        """
        Monitora periodicamente il coordinatore tentando una connessione TCP.
        Se la connessione fallisce (il processo del coordinatore è stato
        terminato), il nodo avvia automaticamente una nuova elezione.
        
        Questo meccanismo implementa la rilevazione del guasto tramite
        timeout reale, come richiesto dalla specifica.
        """
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)

            with self.lock:
                coord = self.coordinator

            # Se non c'è un coordinatore noto, o se sono io il coordinatore,
            # non serve monitorare
            if coord is None or coord == self.node_id:
                with self.lock:
                    self._monitor_active = False
                return

            # Tentativo di connessione TCP al coordinatore
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect((HOST, NODES[coord]))
                s.close()
                # Connessione riuscita: il coordinatore è ancora attivo
            except (ConnectionRefusedError, socket.timeout, OSError):
                # Connessione fallita: il coordinatore è guasto!
                self.log(f"")
                self.log(f"TIMEOUT: coordinatore P{coord} non risponde!")
                self.log(f"")
                with self.lock:
                    self.coordinator = None
                    self._monitor_active = False
                # Avvio automatico di una nuova elezione
                self.start_election()
                return

# INTERFACCIA UTENTE
    def _interactive_menu(self):
        """
        Menu interattivo per controllare il nodo dal terminale.
        Comandi disponibili:
        - 'e': avvia un'elezione manualmente
        - 's': mostra lo stato attuale (coordinatore noto)
        - 'q': termina il nodo
        """
        print()
        print(f"  ╔══════════════════════════════════╗")
        print(f"  ║        NODO P{self.node_id} - COMANDI         ║")
        print(f"  ╠══════════════════════════════════╣")
        print(f"  ║  e = Avvia elezione              ║")
        print(f"  ║  s = Mostra stato                ║")
        print(f"  ║  q = Termina nodo (o Ctrl+C)     ║")
        print(f"  ╚══════════════════════════════════╝")
        print()

        while self.running:
            try:
                cmd = input().strip().lower()
                if cmd == 'e':
                    # Avvia elezione in un thread separato
                    # per non bloccare l'input dell'utente
                    threading.Thread(
                        target=self.start_election,
                        daemon=True
                    ).start()
                elif cmd == 's':
                    with self.lock:
                        coord = self.coordinator
                    if coord is not None:
                        self.log(f"Coordinatore attuale: P{coord}")
                    else:
                        self.log("Nessun coordinatore noto")
                elif cmd == 'q':
                    self.log("*** processo terminato ***")
                    self.running = False
                    break
            except (EOFError, KeyboardInterrupt):
                print()
                self.log("*** processo terminato ***")
                self.running = False
                break


def main():
    """
    Punto di ingresso del programma.
    Legge l'ID del nodo dal primo argomento da riga di comando
    e avvia il nodo corrispondente.
    """
    if len(sys.argv) != 2:
        print("Uso: python node.py <ID_NODO>")
        print("  ID_NODO: 1, 2, 3, 4 o 5")
        print()
        print("Esempio:")
        print("  Terminale 1: python node.py 1")
        print("  Terminale 2: python node.py 2")
        print("  Terminale 3: python node.py 3")
        print("  Terminale 4: python node.py 4")
        print("  Terminale 5: python node.py 5")
        sys.exit(1)

    try:
        node_id = int(sys.argv[1])
        if node_id not in NODES:
            raise ValueError
    except ValueError:
        print("Errore: l'ID del nodo deve essere 1, 2, 3, 4 o 5")
        sys.exit(1)

    # Creazione e avvio del nodo
    node = BullyNode(node_id)
    node.start()


if __name__ == '__main__':
    main()
