#!/usr/bin/env python3

from __future__ import print_function
from . import util
import os
import sys
import time

import pyslurm

DEFAULT_NUM_MACHINES = 1
DEFAULT_TIME = "0:15:00"
DEFAULT_TIMEOUT = 1800


class InvalidNumMachinesException(Exception): pass


class ReservationFailedException(Exception): pass


class ReservationNotFoundException(Exception): pass


class SlurmReservation:
    def __init__(self, reservation_id, partition, script_name, username, state, time, num_machines, assigned_machines):
        self.__reservation_id = reservation_id
        self.__username = username
        self.__partition = partition
        self.__script_name = script_name
        self.__state = state
        self.__time = time
        self.__num_machines = num_machines

        machines = self.split_nodes(assigned_machines)
        self.__assigned_machines = machines

    def split_nodes(self, s):  # parses node strings like r12n[1-30,32] to r12n1, r12n2 ... r12n30, r12n32
        if s is None or len(s) == 0:
            return set()

        s = s.replace("\r\n", "").replace("\n", "").replace("\t", "")

        start = 0
        index = 0
        rack_chunks = []
        in_bracket = False
        while index < len(s):  # Separate them in parts like r12n[1-30,32] or r13n1
            if s[index] == "[":
                in_bracket = True
            elif s[index] == "]":
                in_bracket = False
            elif s[index] == "," and not in_bracket:
                rack_chunks.append(s[start: index])
                start = index + 1
            index += 1
        rack_chunks.append(s[start: index])  # Add the last line

        node_names = set()

        for rack_chunk in rack_chunks:
            if "[" in rack_chunk:
                prefix, postfix = rack_chunk.split("[")
                postfix = postfix[:-1]  # Remove the last bracket
                nodes = postfix.split(",")
                for node in nodes:
                    if "-" in node:
                        start, end = node.split("-")
                        if not start.isnumeric() or not end.isnumeric():
                            continue
                        for i in range(int(start), int(end) + 1):
                            node_names.add("{}{}".format(prefix, i))
                    else:
                        node_names.add("{}{}".format(prefix, node))
            else:
                node_names.add(rack_chunk)

        return node_names

    @property
    def reservation_id(self):
        return self.__reservation_id

    @property
    def queue(self):
        return self.__partition

    @property
    def script_name(self):
        return self.__script_name

    @property
    def username(self):
        return self.__username

    @property
    def state(self):
        return self.__state

    @property
    def time(self):
        return self.__time

    @property
    def num_machines(self):
        return self.__num_machines

    @property
    def assigned_machines(self):
        return self.__assigned_machines


def _parse_int_or_default(value, default_value):
    try:
        return int(value)
    except ValueError:
        return default_value


def SlurmReservation_from_squeue_line(line):
    parts = line.split()
    return SlurmReservation(
        reservation_id=parts[0],
        partition=parts[1],
        script_name=parts[2],
        username=parts[3],
        state=parts[4],
        time=parts[5],
        num_machines=_parse_int_or_default(parts[6], 0),
        assigned_machines=parts[8]
    )


class SlurmManager:
    def __init__(self, username):
        self.__username = username

    @property
    def username(self):
        return self.__username

    def get_reservations(self):
        list_output = util.execute_command_for_output(["squeue"]).split("\n")
        reservations = {}
        found_header = False
        for line in list_output.split('\n'):
            line = line.strip()
            if not found_header:
                if line.startswith("JOBID"):
                    found_header = True
            elif line.strip():
                reservation = SlurmReservation_from_squeue_line(line)
                reservations[reservation.reservation_id] = reservation
        return reservations

    def get_own_reservations(self):
        reservations = self.get_reservations()
        return {k: v for k, v in reservations.items() if v.username == self.username}

    def create_reservation(self, num_machines, time, partition=None):
        if num_machines < 1:
            raise InvalidNumMachinesException("Number of machines must be at least one.")

        epoch_now = int(time.time())

        res = pyslurm.reservation()
        res_dict = pyslurm.create_reservation_dict()
        res_dict["node_cnt"] = 1
        res_dict["users"] = os.environ['USER']
        if partition:
            res_dict["partition"] = partition
        res_dict["start_time"] = epoch_now
        res_dict["duration"] = time
        res_dict["name"] = "res_test"

        reservation_id = res.create(res_dict)
        return reservation_id

        # # Invoke preserve to make the reservation
        # command = ["salloc", "-N", str(num_machines), "-t", time]
        # if partition is not None:
        #     command.extend(["-p", partition])
        # reservation_output = util.execute_command_for_output(command)
        #
        # # Extract the reservation ID
        # reservation_id = None
        # for line in reservation_output.split('\n'):
        #     if line.startswith("salloc:"):
        #         reservation_id = line.split(" ")[-1]
        #         break
        # if reservation_id is None:
        #     raise ReservationFailedException(
        #         "preserve did not print a reservation id. Output:\n%s" % reservation_output)
        #
        # return int(reservation_id)

    def fetch_reservation(self, reservation_id):
        if reservation_id.upper() == "LAST":
            reservations = self.get_own_reservations()
            if len(reservations) == 0:
                raise ReservationNotFoundException("Could not retrieve last reservation; no reservations were found.")
            reservation_ids = sorted(reservations.keys())
            return reservations[reservation_ids[-1]]
        else:
            reservations = self.get_reservations()
            if not int(reservation_id) in reservations:
                raise ReservationNotFoundException('Could not find reservation for id "%s".' % reservation_id)
            return reservations[int(reservation_id)]

    def kill_reservation(self, reservation_id):
        reservation = self.fetch_reservation(reservation_id)
        if not reservation.username == self.username:
            raise ReservationNotFoundException("Reservation for given id does not belong to the user.")
        util.execute_command_quietly(["scancel", str(reservation.reservation_id)])


def add_slurm_subparser(parser):
    slurm_parser = parser.add_parser("salloc", help="manage reservations using slurm")
    slurm_subparsers = slurm_parser.add_subparsers(title="Reservation management commands")

    # Add subparser for "create-reservation" command
    create_parser = slurm_subparsers.add_parser("create-reservation", help="reserve machines")
    create_parser.add_argument("-t", "--time",
                               help="time to reserve machines for as [[hh:]mm:]ss (default: %s)" % DEFAULT_TIME,
                               action="store", default=DEFAULT_TIME)
    create_parser.add_argument("-q", "--quiet", help="output only the reservation id", action="store_true")
    create_parser.add_argument("-p", "--partition", help="partition (queue) the job is submitted to", action="store_true")
    create_parser.add_argument("NUM_MACHINES", help="number of machines to reserve", action="store", type=int)
    create_parser.set_defaults(func=__create_reservation)

    # Add subparser for "list-reservations" command
    list_parser = slurm_subparsers.add_parser("list-reservations", help="list active reservations through squeue")
    list_parser.add_argument("-a", "--all", help="list reservations for all users", action="store_true")
    list_parser.set_defaults(func=__list_reservations)

    # Add subparser for "fetch-reservation" command
    fetch_parser = slurm_subparsers.add_parser("fetch-reservation",
                                               help="display information on a single reservation")
    fetch_parser.add_argument("RESERVATION_ID",
                              help="id of the reservation to display, or 'LAST' to fetch the last reservation made by the user",
                              action="store")
    fetch_parser.set_defaults(func=__fetch_reservation)

    # Add subparser for "wait-for-reservation" command
    wait_parser = slurm_subparsers.add_parser("wait-for-reservation",
                                              help="wait for a reservation to enter the ready state")
    wait_parser.add_argument("-t", "--timeout",
                             help="maximum time in seconds to wait before exiting with an error code (default: %u)" % DEFAULT_TIMEOUT,
                             action="store", default=DEFAULT_TIMEOUT)
    wait_parser.add_argument("-q", "--quiet", help="do not output anything", action="store_true")
    wait_parser.add_argument("RESERVATION_ID",
                             help="id of the reservation to wait for, or 'LAST' to wait for the last reservation made by the user",
                             action="store")
    wait_parser.set_defaults(func=__wait_for_reservation)

    # Add subparser for "kill-reservation" command
    kill_parser = slurm_subparsers.add_parser("kill-reservation", help="terminate and clean up a reservation")
    kill_parser.add_argument("RESERVATION_ID",
                             help="id of the reservation to kill, or 'LAST' to kill the last reservation made by the user",
                             action="store")
    kill_parser.set_defaults(func=__kill_reservation)


def get_SlurmManager():
    return SlurmManager(os.environ["USER"])


def __create_reservation(args):
    epoch_now = int(time.time())

    res = pyslurm.reservation()
    res_dict = pyslurm.create_reservation_dict()
    res_dict["node_cnt"] = 1
    res_dict["users"] = os.environ['USER']
    res_dict["partition"] = args.partition
    res_dict["start_time"] = epoch_now
    res_dict["duration"] = args.time
    res_dict["name"] = "res_test"

    reservation_id = res.create(res_dict)
    if args.quiet:
        print(reservation_id)
    else:
        print("Reservation succesful. Reservation ID is %s." % reservation_id)


def __list_reservations(args):
    sm = get_SlurmManager()
    if args.all:
        reservations = sm.get_reservations()
        print("id\tusername\tstate\tnum_machines")
        for reservation in reservations.values():
            print("%s\t%s\t%s\t%s" % (
                reservation.reservation_id, reservation.username, reservation.state, reservation.num_machines))
    else:
        reservations = sm.get_own_reservations()
        print("id\tstate\tnum_machines")
        for reservation in reservations.values():
            print("%s\t%s\t%s" % (reservation.reservation_id, reservation.state, reservation.num_machines))


def __fetch_reservation(args):
    sm = get_SlurmManager()
    reservation = sm.fetch_reservation(args.RESERVATION_ID)

    print("Reservation ID: %s" % reservation.reservation_id)
    print("State:          %s" % reservation.state)
    print("Start time:     %s" % reservation.start_time)
    print("End time:       %s" % reservation.end_time)
    if len(reservation.assigned_machines) > 0:
        print("Machines:       %s" % " ".join(reservation.assigned_machines))
    else:
        print("Req. machines:  %d" % reservation.num_machines)


def __wait_for_reservation(args):
    sm = get_SlurmManager()
    starttime = time.time()
    lasttime = starttime + int(args.timeout)

    waittime = 5
    timeswaited = 0

    while True:
        state = sm.fetch_reservation(args.RESERVATION_ID).state
        if state == "R":
            break

        curtime = time.time()
        maxwaittime = lasttime - curtime
        nextwaittime = int(min(maxwaittime, waittime))
        if nextwaittime <= 0:
            print("[%.1f] Current state: %s. Reached timeout." % (curtime, state))
            sys.exit("wait-for-reservation timed out")
        if not args.quiet:
            print("[%.1f] Current state: %s. Waiting %u more seconds." % (curtime, state, nextwaittime))
        time.sleep(nextwaittime)

        timeswaited += 1
        if timeswaited == 12:
            waittime = 10  # After a minute, decrease the polling frequency
        elif timeswaited == 36:
            waittime = 15  # After 5 minutes, decrease the polling frequency
        elif timeswaited == 76:
            waittime = 30  # After 15 minutes, decrease the polling frequency


def __kill_reservation(args):
    try:
        pyslurm.reservation().delete(args.RESERVATION_ID)
    except ValueError as value_error:
        print("Reservation ({0}) delete failed - {1}".format(args.RESERVATION_ID, value_error.args[0]))
    else:
        print("Reservation {0} deleted".format(args.RESERVATION_ID))
