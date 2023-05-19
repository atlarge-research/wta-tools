#!/usr/bin/env python2

from __future__ import print_function
from . import util
import argparse
import os
import sys
import time

DEFAULT_NUM_MACHINES=1
DEFAULT_TIME="0:15:00"
DEFAULT_TIMEOUT=1800

class InvalidNumMachinesException(Exception): pass
class ReservationFailedException(Exception): pass
class ReservationNotFoundException(Exception): pass

class PreserveReservation:
    def __init__(self, reservation_id, username, start_time, end_time, state, num_machines, assigned_machines):
        self.__reservation_id = reservation_id
        self.__username = username
        self.__start_time = start_time
        self.__end_time = end_time
        self.__state = state
        self.__num_machines = num_machines
        self.__assigned_machines = assigned_machines

    @property
    def reservation_id(self):
        return self.__reservation_id

    @property
    def username(self):
        return self.__username

    @property
    def start_time(self):
        return self.__start_time

    @property
    def end_time(self):
        return self.__end_time

    @property
    def state(self):
        return self.__state

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

def PreserveReservation_from_preserve_line(line):
    parts = line.split()
    return PreserveReservation(
            reservation_id=int(parts[0]),
            username=parts[1],
            start_time="%s %s" % (parts[2], parts[3]),
            end_time="%s %s" % (parts[4], parts[5]),
            state=parts[6],
            num_machines=_parse_int_or_default(parts[7], 0),
            assigned_machines=sorted(["%s.ib.cluster" % part for part in parts[8:]])
        )

class PreserveManager:
    def __init__(self, username):
        self.__username = username

    @property
    def username(self):
        return self.__username

    def get_reservations(self):
        list_output = util.execute_command_for_output(["preserve", "-llist"])
        reservations = {}
        found_header = False
        for line in list_output.split('\n'):
            if not found_header:
                if line.startswith("id"):
                    found_header = True
            elif line.strip():
                reservation = PreserveReservation_from_preserve_line(line)
                reservations[reservation.reservation_id] = reservation
        return reservations

    def get_own_reservations(self):
        reservations = self.get_reservations()
        return { k: v for k, v in reservations.iteritems() if v.username == self.username }

    def create_reservation(self, num_machines, time):
        if num_machines < 1:
            raise InvalidNumMachinesException("Number of machines must be at least one.")

        # Invoke preserve to make the reservation
        reservation_output = util.execute_command_for_output(["preserve", "-np", str(num_machines), "-t", time])

        # Extract the reservation ID 
        reservation_id = None
        for line in reservation_output.split('\n'):
            if line.startswith("Reservation number"):
                reservation_id = line.strip(":").split(" ")[-1]
                break
        if reservation_id is None:
            raise ReservationFailedException("preserve did not print a reservation id. Output:\n%s" % reservation_output)

        return int(reservation_id)

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
        util.execute_command_quietly(["preserve", "-c", str(reservation.reservation_id)])

def add_preserve_subparser(parser):
    preserve_parser = parser.add_parser("preserve", help="manage reservations using preserve")
    preserve_subparsers = preserve_parser.add_subparsers(title="Reservation management commands")
    
    # Add subparser for "create-reservation" command
    create_parser = preserve_subparsers.add_parser("create-reservation", help="reserve machines")
    create_parser.add_argument("-t", "--time", help="time to reserve machines for as [[hh:]mm:]ss (default: %s)" % DEFAULT_TIME, action="store", default=DEFAULT_TIME)
    create_parser.add_argument("-q", "--quiet", help="output only the reservation id", action="store_true")
    create_parser.add_argument("NUM_MACHINES", help="number of machines to reserve", action="store", type=int)
    create_parser.set_defaults(func=__create_reservation)

    # Add subparser for "list-reservations" command
    list_parser = preserve_subparsers.add_parser("list-reservations", help="list active reservations through preserve")
    list_parser.add_argument("-a", "--all", help="list reservations for all users", action="store_true")
    list_parser.set_defaults(func=__list_reservations)

    # Add subparser for "fetch-reservation" command
    fetch_parser = preserve_subparsers.add_parser("fetch-reservation", help="display information on a single reservation")
    fetch_parser.add_argument("RESERVATION_ID", help="id of the reservation to display, or 'LAST' to fetch the last reservation made by the user", action="store")
    fetch_parser.set_defaults(func=__fetch_reservation)

    # Add subparser for "wait-for-reservation" command
    wait_parser = preserve_subparsers.add_parser("wait-for-reservation", help="wait for a reservation to enter the ready state")
    wait_parser.add_argument("-t", "--timeout", help="maximum time in seconds to wait before exiting with an error code (default: %u)" % DEFAULT_TIMEOUT, action="store", default=DEFAULT_TIMEOUT)
    wait_parser.add_argument("-q", "--quiet", help="do not output anything", action="store_true")
    wait_parser.add_argument("RESERVATION_ID", help="id of the reservation to wait for, or 'LAST' to wait for the last reservation made by the user", action="store")
    wait_parser.set_defaults(func=__wait_for_reservation)

    # Add subparser for "kill-reservation" command
    kill_parser = preserve_subparsers.add_parser("kill-reservation", help="terminate and clean up a reservation")
    kill_parser.add_argument("RESERVATION_ID", help="id of the reservation to kill, or 'LAST' to kill the last reservation made by the user", action="store")
    kill_parser.set_defaults(func=__kill_reservation)

def get_PreserveManager():
    return PreserveManager(os.environ["USER"])

def __create_reservation(args):
    pm = get_PreserveManager()
    reservation_id = pm.create_reservation(args.NUM_MACHINES, args.time)
    if args.quiet:
        print(reservation_id)
    else:
        print("Reservation succesful. Reservation ID is %s." % reservation_id)

def __list_reservations(args):
    pm = get_PreserveManager()
    if args.all:
        reservations = pm.get_reservations()
        print("id\tusername\tstate\tnum_machines")
        for reservation in reservations.values():
            print("%s\t%s\t%s\t%s" % (reservation.reservation_id, reservation.username, reservation.state, reservation.num_machines))
    else:
        reservations = pm.get_own_reservations()
        print("id\tstate\tnum_machines")
        for reservation in reservations.values():
            print("%s\t%s\t%s" % (reservation.reservation_id, reservation.state, reservation.num_machines))

def __fetch_reservation(args):
    pm = get_PreserveManager()
    reservation = pm.fetch_reservation(args.RESERVATION_ID)

    print("Reservation ID: %s" % reservation.reservation_id)
    print("State:          %s" % reservation.state)
    print("Start time:     %s" % reservation.start_time)
    print("End time:       %s" % reservation.end_time)
    if len(reservation.assigned_machines) > 0:
        print("Machines:       %s" % " ".join(reservation.assigned_machines))
    else:
        print("Req. machines:  %d" % reservation.num_machines)

def __wait_for_reservation(args):
    pm = get_PreserveManager()
    starttime = time.time()
    lasttime = starttime + int(args.timeout)

    waittime = 5
    timeswaited = 0

    while True:
        state = pm.fetch_reservation(args.RESERVATION_ID).state
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
            waittime = 10 # After a minute, decrease the polling frequency
        elif timeswaited == 36:
            waittime = 15 # After 5 minutes, decrease the polling frequency
        elif timeswaited == 76:
            waittime = 30 # After 15 minutes, decrease the polling frequency

def __kill_reservation(args):
    pm = get_PreserveManager()
    pm.kill_reservation(args.RESERVATION_ID)
