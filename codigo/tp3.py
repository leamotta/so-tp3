# -*- coding: utf-8 -*-

import hashlib
import random
import sys
import time

from mpi4py import MPI


NODE_CONSOLE_RANK = 0    # Nodo consola, no forma parte de la red.
NODE_CONTACT_RANK = 1    # Nodo de contacto inicial al cual se conectan los demás nodos al unirse a la red.

# Protocolo CONSOLE
TAG_CONSOLE_JOIN_REQ = 11

TAG_CONSOLE_STORE_REQ = 12

TAG_CONSOLE_LOOKUP_REQ = 13
TAG_CONSOLE_LOOKUP_RESP = 14

TAG_CONSOLE_EXIT_REQ = 15

# Protocolo NODE
TAG_NODE_FIND_NODES_JOIN_REQ = 21
TAG_NODE_FIND_NODES_JOIN_RESP = 22

TAG_NODE_FIND_NODES_REQ = 23
TAG_NODE_FIND_NODES_RESP = 24

TAG_NODE_LOOKUP_REQ = 25
TAG_NODE_LOOKUP_RESP = 26

TAG_NODE_STORE_REQ = 27

# Tamaño de mi tabla
K = 8

def tag_to_string(tag):
    name_map = {
        # CONSOLE
        TAG_CONSOLE_JOIN_REQ: "TAG_CONSOLE_JOIN_REQ",
        TAG_CONSOLE_STORE_REQ: "TAG_CONSOLE_STORE_REQ",
        TAG_CONSOLE_LOOKUP_REQ: "TAG_CONSOLE_LOOKUP_REQ",
        TAG_CONSOLE_LOOKUP_RESP: "TAG_CONSOLE_LOOKUP_RESP",
        TAG_CONSOLE_EXIT_REQ: "TAG_CONSOLE_EXIT_REQ",

        # NODE
        TAG_NODE_FIND_NODES_JOIN_REQ: "TAG_NODE_FIND_NODES_JOIN_REQ",
        TAG_NODE_FIND_NODES_JOIN_RESP: "TAG_NODE_FIND_NODES_JOIN_RESP",
        TAG_NODE_FIND_NODES_REQ: "TAG_NODE_FIND_NODES_REQ",
        TAG_NODE_FIND_NODES_RESP: "TAG_NODE_FIND_NODES_RESP",
        TAG_NODE_LOOKUP_REQ: "TAG_NODE_LOOKUP_REQ",
        TAG_NODE_LOOKUP_RESP: "TAG_NODE_LOOKUP_RESP",
        TAG_NODE_STORE_REQ: "TAG_NODE_STORE_REQ",


    }

    return name_map[tag]


def distance(x, y):
    return bin(x ^ y).count("1")


def hash_fn(x):
    return long(hashlib.sha256(x).hexdigest(), 16)


class Node(object):

    def __init__(self, node_rank, node_id):
        self.__comm = MPI.COMM_WORLD
        self.__files = {}
        self.__finished = False
        self.__hash = hash_fn(str(node_id))
        self.__id = node_id
        self.__initialized = False
        self.__rank = node_rank
        self.__routing_table = {}

    def __get_min(self, nodes, thing_hash):
        return self.__get_mins(nodes, thing_hash)[0]

    def __get_mins(self, nodes, thing_hash):

        # Calculo mínimo.
        dist_min = sys.maxint
        for node_hash in nodes:
            if distance(node_hash, thing_hash) < dist_min:
                dist_min = distance(node_hash, thing_hash)

        # Retorno: [(node_hash, node_rank)]
        return [(node_hash, node_rank) for node_hash, node_rank in nodes.items()
                    if distance(node_hash, thing_hash) == dist_min]

    def __get_local_mins(self, thing_hash):
        return self.__get_mins(self.__routing_table, thing_hash)

    def __get_maxs(self, nodes, thing_hash):
        # nodes = dict(node_hash: node_rank)

        # Calculo máximo.
        dist_max = 0
        for node_hash in nodes:
            if distance(node_hash, thing_hash) > dist_max:
                dist_max = distance(node_hash, thing_hash)

        # Retorno: [(node_hash, node_rank)]
        return [(node_hash, node_rank) for node_hash, node_rank in nodes.items()
                    if distance(node_hash, thing_hash) == dist_max]

    def __get_local_max(self, thing_hash):
        return self.__get_maxs(self.__routing_table, thing_hash)

    def __update_routing_table(self, node_hash, node_rank):
        if len(self.__routing_table) < K:
            self.__routing_table[node_hash] = node_rank
        else:
            # Busco los nodos con más distancia a mi.
            nodes_max = self.__get_local_max(self.__hash)

            node_max_hash, node_max_rank = nodes_max[0]

            if distance(self.__hash, node_hash) < distance(self.__hash, node_max_hash):
                del self.__routing_table[node_max_hash]
                self.__routing_table[node_hash] = node_rank

    # Le pregunta a los nodos mínimos por el hash
    # Hace la pregunta de forma recursiva (a los nodos de mínima distancia
    # que se le pasan y a que van siendo mínima de los que obtiene de consultarle a cada uno
    def __find_nodes(self, contact_nodes, thing_hash):
        queue = contact_nodes
        processed = set()
        nodes_min = {}

        # el minimo al empezar soy yo
        min_distance = distance(self.__hash, thing_hash) 
        nodes_min[self.__rank] = self.__hash

        #me agrego al set de procesados
        processed.add((self.__hash, self.__rank))

        # proceso los iniciales
        for node in queue:
            processed.add(node)

        # proceso los siguientes
        for node in queue:
            if (distance(node[0], thing_hash) < min_distance):
                nodes_min = {node[1]: node[0]}
                min_distance = distance(node[0], thing_hash)
            elif (distance(node[0], thing_hash) == min_distance):
                nodes_min[node[1]] = node[0]

            if (node[1] != self.__rank):# no me mando mensaje a mi mismo
                self.__comm.send(thing_hash, dest=node[1], tag=TAG_NODE_FIND_NODES_REQ)
                # waiting
                recieved_node_list = self.__comm.recv(source=node[1], tag=TAG_NODE_FIND_NODES_RESP)

                # hay que encolar a los nodos recibidos no procesados previamente
                for node_recieved in recieved_node_list:
                    if not node_recieved in processed:
                        # sumo a la cola
                        queue.append(node_recieved) 
                        # sumo a los procesados
                        processed.add(node_recieved) 

        return nodes_min

    # casi igual a find_node pero cada nodo va borrando los archivos que ya estarían más cercano al find nodes
    # o copiando los mismos si es igual la distaancia. Además le devuelve los archivos 
    # que le correspondería tener a él
    def __find_nodes_join(self, contact_nodes):
        nodes_min = set()
    ################
    # Completar
    ################
        return nodes_min

    def __print_routing_table(self):
        print("[D] Routing table nodo : {:02d} :: {}".format(self.__rank, self.__routing_table))

    def __get_closest_files(self, node_hash):
        # devuelve los archivos en self.__files que son más cercanos a node_hash que a self.__hash
        files = {}
        print self.__files
        for file_hash, file_name in self.__files.items():
            if distance (file_hash, self.__hash) > distance (file_hash, node_hash):
                files[file_hash] = file_name
        return files

    def __get_equal_files(self, node_hash):
        # devuelve los archivos en self.__files que son igual de cercanos a node_hash que a self.__hash
        files = {}
        print self.__files
        for file_hash, file_name in self.__files.items():
            if distance (file_hash, self.__hash) == distance (file_hash, node_hash):
                files[file_hash] = file_name
        return files



    # Handlers del protocolo CONSOLE.
    # ======================================================================== #
    def __handle_console_join(self, contact_node_rank):
        # Debe agregar al nodo con rank NODE_CONTACT_RANK a su routing table.
        # El find_nodes que se usa acá debe propagar la info de que este
        # es un nuevo nodo.

        print("[D] [{:02d}] [CONSOLE|JOIN] Uniendo el nodo actual al nodo '{}'".format(self.__rank, contact_node_rank))

        # Si yo soy el contacto inicial, inicio por default
        if self.__rank == contact_node_rank:
            self.__initialized = True

        if not self.__initialized:
            # Pregunto al nodo inicial cuales son los nodos más cercanos a mi.
            data = (self.__hash, self.__rank)
            self.__comm.send(data, dest=contact_node_rank, tag=TAG_NODE_FIND_NODES_JOIN_REQ)

            # Recibir respuesta por find nodes (data = [(node_hash, node_rank)])
            # files es un dicc con los archivos de node del que soy el nuevo nodo más cercano
            (data, files) = self.__comm.recv(source=contact_node_rank, tag=TAG_NODE_FIND_NODES_JOIN_RESP)

            # Agrego los archivos recibidos a self.__files
            for file_hash, file_name in files.items():
                self.__files[file_hash] = file_name

            # Propago consulta de find nodes a traves de los minimos de mi nodo
            # de contacto inicial.
            nodes_min = self.__find_nodes_join(data)  
            # expando con un dato que sea la distancia.
            nodes_min = [(node_hash, node_rank, distance(node_hash, self.__hash)) for node_hash, node_rank in nodes_min]

            # obtengo los K mínimos
            nodes_min = sorted(nodes_min, key=lambda x: x[2])

            ## Convierto set a dict.
            #nodes_min = {node_hash: node_rank for node_hash, node_rank, distancia in nodes_min}


            # Me quedo con los K más cercanos. 
            for index, (node_hash, node_rank, distancia) in enumerate(nodes_min):
                if index < K:
                    self.__routing_table[node_hash] = node_rank
                else:
                    break

            print("[D] [{:02d}] [CONSOLE|JOIN] Tabla de ruteo: {}".format(self.__rank, self.__routing_table))

            print("[D] [{:02d}] [CONSOLE|JOIN] Tabla de archivos: {}".format(self.__rank, self.__files))

            self.__initialized = True

        print("[+] Inicializacion completa del nodo '{:02d}'".format(self.__rank))

    #  Si soy el nodo de menor distancia, lo guardo yo, si tengo vecinos de igual distancia, tambien se los
    # mando a guardar a ellos. Si tengo alguno de menor distancia, se lo mando a guardar.
    def __handle_console_store(self, data):
        # IMPORTANTE El store va a generar MSJs entre nodos el cual necesita
        # VARIAS respuesta que se procesa en este ciclo (más abajo).
        file_hash, file_name = data

        print("[D] [{:02d}] [CONSOLE|STORE] Almacenando archivo '{}' con hash '{}'".format(self.__rank, file_name, file_hash))

        # Obtengo minimos locales.
        nodes_min_local = self.__get_local_mins(file_hash)

        print("[D] [{:02d}] [CONSOLE|STORE] Buscando nodo más cercano para almacenando archivo '{}' con hash '{}'".format(self.__rank, file_name, file_hash))

        # Propago consulta de find nodes a traves de mis minimos locales.
        nodes_min = self.__find_nodes(nodes_min_local, file_hash)
    ########################
    #     Completar
    ########################

            # Envio el archivo a los nodos más cercanos

    # Busca el archivo entre los más cercanos, a partir del nodo fuente. Les va consultando a cada uno los más cercanos
    # con __finde_nodes
    def __handle_console_look_up(self, source, data):
        # IMPORTANTE El store va a generar MSJs entre nodos el cual necesita
        # VARIAS respuesta que se procesa en este ciclo (más abajo).
        file_hash = data

        print("[D] [{:02d}] [CONSOLE|LOOK-UP] Buscando archivo con hash '{}'".format(self.__rank, file_hash))

        # Obtengo minimos locales.
        nodes_min_local = self.__get_local_mins(file_hash)

        # Propago consulta de find nodes a traves de mis minimos locales.
        nodes_min = self.__find_nodes(nodes_min_local, file_hash)

    ########################
    #     Completar
    ########################
        # Devuelvo el archivo.
        self.__comm.send(data, dest=source, tag=TAG_CONSOLE_LOOKUP_RESP)

    def __handle_console_finish(self, data):
        self.__finished = True

    # Handlers del protocolo NODE.
    # ======================================================================== #
    def __handle_node_join(self, data):
        # Se incorporo un nuevo nodo a la red. Este mensaje pregunta por
        # los nodos más cercanos y, en caso que corresponda, indica que
        # se debe agregar el nuevo nodo a su tabla de routing.

        node_hash, node_rank = data

        print("[D] [{:02d}] [NODE|JOIN] Uniendo el nodo actual al nodo '{}' con hash '{}'".format(self.__rank, node_rank, node_hash))

        nodes_min = self.__get_local_mins(node_hash)

        # Agrego ARBITRARIAMENTE al nodo actual.
        nodes_min.append((self.__hash, self.__rank))

        # Busco entre mis archivos los más cercanos a node que a mí.
        files_menor = self.__get_closest_files(node_hash)
        files_menor_igual = self.__get_equal_files(node_hash)
    files_menor_igual.update(files_menor)
        # Envio los nodos más cercanos y los archivos más cercanos a node que tenía yo
        data = (nodes_min, files_menor_igual)
        self.__comm.send(data, dest=node_rank, tag=TAG_NODE_FIND_NODES_JOIN_RESP)

        # Borro de mis archivos los más cercanos a node
        self.__files = {k:v for k,v in self.__files.items() if k not in files_menor.keys()}

        # Actualizo la routing table.
        self.__update_routing_table(node_hash, node_rank)

        print("[D] [{:02d}] [NODE|JOIN] Tabla de ruteo: {}".format(self.__rank, self.__routing_table))

    def __handle_node_find_nodes(self, source, data):

        # thing_hash puede ser el hash de un archivo o de un nodo.
        thing_hash = data

        print("[D] [{:02d}] [NODE|FIND-NODES] Buscando nodos más cercanos al hash '{}' pedido por el nodo '{}'".format(self.__rank, thing_hash, source))

        nodes_min = self.__get_local_mins(thing_hash)

        # Agrego ARBITRARIAMENTE al nodo actual.
        nodes_min.append((self.__hash, self.__rank))

        print("[D] [{:02d}] [NODE|FIND-NODES] Nodos más cercanos: '{}'".format(self.__rank, nodes_min))

        # data es de tipo [(node_hash, node_rank)]
        data = nodes_min
        self.__comm.send(data, dest=source, tag=TAG_NODE_FIND_NODES_RESP)

    def __handle_node_look_up(self, source, data):
        # Envio archivo.
        file_hash = data

        print("[D] [{:02d}] [NODE|LOOK-UP] Buscando archivo con hash '{}'".format(self.__rank, file_hash))
        print("[D] [{:02d}] [NODE|LOOK-UP] Tabla de archivos: {}".format(self.__rank, self.__files))

        data = self.__files[file_hash]
        self.__comm.send(data, dest=source, tag=TAG_NODE_LOOKUP_RESP)

    def __handle_node_store_req(self, data):
        # Recibo archivo.
        file_hash, file_name = data

        print("[D] [{:02d}] [NODE|STORE] Almacenando archivo '{}' con hash '{}'".format(self.__rank, file_name, file_hash))

        self.__files[file_hash] = file_name

        print("[D] [{:02d}] [NODE|STORE] Tabla de archivos: {}".format(self.__rank, self.__files))

    def run(self):
        print("[+] Iniciando nodo '{:02d}'.".format(self.__rank))

        status = MPI.Status()

        while not self.__finished:
            data = self.__comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()
            source = status.Get_source()

            print("[D] [{:02d}] recv | data: {}, source: {}, tag: {}".format(self.__rank, data, source, tag_to_string(tag)))

            # El primer mensaje que espera es el de inicializacion (o que exit).
            if not self.__initialized and tag not in [TAG_CONSOLE_JOIN_REQ, TAG_CONSOLE_EXIT_REQ]:
                print("[D] [{:02d}] Nodo no inicializado, IGNORANDO mensaje...".format(self.__rank))
                continue

            # Protocolo CONSOLE
            # ================================================================ #
            if tag == TAG_CONSOLE_JOIN_REQ:
                self.__handle_console_join(data)
                continue
            elif tag == TAG_CONSOLE_STORE_REQ:
                self.__handle_console_store(data)
                continue
            elif tag == TAG_CONSOLE_LOOKUP_REQ:
                self.__handle_console_look_up(source, data)
                continue
            elif tag == TAG_CONSOLE_EXIT_REQ:
                self.__handle_console_finish(data)
                continue

            # Protocolo NODE
            # ================================================================ #
            if tag == TAG_NODE_FIND_NODES_JOIN_REQ:
                self.__handle_node_join(data)
                continue
            elif tag == TAG_NODE_FIND_NODES_REQ:
                self.__handle_node_find_nodes(source, data)
                continue
            elif tag == TAG_NODE_LOOKUP_REQ:
                self.__handle_node_look_up(source, data)
                continue
            elif tag == TAG_NODE_STORE_REQ:
                self.__handle_node_store_req(data)
                continue

        print("[+] Finalizando nodo '{:02d}'".format(self.__rank))


class Console(object):

    def __init__(self, rank):
        self.__comm = MPI.COMM_WORLD
        self.__finished = False
        self.__rank = rank

    def run(self):
        print("[+] Iniciando consola (rank: {:02d})".format(self.__rank))

        while not self.__finished:
            command, args = self.__parse_command(raw_input("> "))

            if command == "quit":
                self.__handle_quit(*args)
            elif command == "join":
                self.__handle_join(*args)
            elif command == "store":
                self.__handle_store(*args)
            elif command == "look-up":
                self.__handle_look_up(*args)
            elif command == "test":
                self.__handle_test(*args)
            else:
                self.__print_usage()
                continue

        print("[+] Finalizando consola")

    def __parse_command(self, command_string):
        tokens = [token.strip().lower() for token in command_string.split(" ") if token]

        if tokens[0] in ["q", "quit"]:
            command = "quit"
            args = []
        elif tokens[0] in ["j", "join"]:
            command = "join"
            node_rank = int(tokens[1])
            args = [node_rank]
        elif tokens[0] in ["s", "store"]:
            command = "store"
            node_rank = int(tokens[1])
            file_name = tokens[2]
            args = [node_rank, file_name]
        elif tokens[0] in ["l", "look-up"]:
            command = "look-up"
            node_rank = int(tokens[1])
            file_name = tokens[2]
            args = [node_rank, file_name]
        elif tokens[0] in ["t", "test"]:
            command = "test"
            commands_file = tokens[1]
            args = [commands_file]
        else:
            command = None
            args = []

        return command, args

    def __handle_quit(self):
        """Procesa comando: 'quit'.
        """
        print(">>> Saliendo...")
        for i in range(MPI.COMM_WORLD.Get_size()):
                data = None
                self.__comm.isend(data, dest=i, tag=TAG_CONSOLE_EXIT_REQ)

        self.__finished = True

    def __handle_join(self, node_rank):
        """Procesa comando: 'join <node_rank>'.
        """
        print(">>> Uniendo nodo '{:02d}' a la red...".format(node_rank))

        data = NODE_CONTACT_RANK
        self.__comm.send(data, dest=node_rank, tag=TAG_CONSOLE_JOIN_REQ)

    def __handle_store(self, node_rank, file_name):
        """Procesa comando: 'store <node_rank> <file_name>'.
        """
        print(">>> Almacenando archivo '{}' en nodo '{:02d}'...".format(file_name, node_rank))

        data = (hash_fn(file_name), file_name)
        self.__comm.send(data, dest=node_rank, tag=TAG_CONSOLE_STORE_REQ)

    def __handle_look_up(self, node_rank, file_name):
        """Procesa comando: 'look-up <node_rank> <file_name>'.
        """
        print(">>> Recuperando archivo '{}' del nodo '{:02d}'...".format(file_name, node_rank))

        # Enviar pedido de LOOK-UP.
        data = hash_fn(file_name)
        self.__comm.send(data, dest=node_rank, tag=TAG_CONSOLE_LOOKUP_REQ)

        # Recibir pedido de LOOK-UP.
        data = self.__comm.recv(source=node_rank, tag=TAG_CONSOLE_LOOKUP_RESP)

        if data == file_name:
            print(">>> Archivo recibido correctamente!")
        else:
            print(">>> Error al recibir el archivo: {}".format(data))

    def __handle_test(self, commands_file):
        """Procesa comando: 'test <commands_file>'.
        """
        print(">>> Abriendo archivo de comandos para test {}...".format(commands_file))
        f = open(commands_file, 'r')

        i = 1
        for line in f:
            print("Comando {}> {}".format(i, line))
            command, args = self.__parse_command(line)
            if command == "quit":
                self.__handle_quit(*args)
            elif command == "join":
                self.__handle_join(*args)
            elif command == "store":
                self.__handle_store(*args)
            elif command == "look-up":
                self.__handle_look_up(*args)
            i = i+1
            time.sleep(1)


    def __print_usage(self):
        usage = """
        Los comandos disponibles son:
            * j|join <node_rank>
            * s|store <node_rank> <file_name>
            * l|look-up <node_rank> <file_name>
            * q|quit
            * t|test <commands_file>
        """
        print(usage)


if __name__ == "__main__":
    # Ojo Los 'print'. Se muestran mucho después

    size = MPI.COMM_WORLD.Get_size()
    rank = MPI.COMM_WORLD.Get_rank()

    if rank == NODE_CONSOLE_RANK:
        console = Console(rank)
        console.run()
    else:
        # node_id = int(random.uniform(0, 2**K))
        node_id = rank 
        node = Node(rank, node_id)
        node.run()