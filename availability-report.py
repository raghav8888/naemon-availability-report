# Create a MySQL view using the below command:

# CREATE VIEW service_monitor_time_range AS select hostname, service_description, min(start_time) AS min_start_time,max(start_time) AS max_start_time
#   from statusengine_servicechecks group by hostname, service_description


import mysql.connector
from mysql.connector import errorcode
import datetime
import time
import argparse
from azure.storage.blob import *

# Convert seconds of each state into human readable format
def secondsToText(secs):
    days = secs//86400
    hours = (secs - days*86400)//3600
    minutes = (secs - days*86400 - hours*3600)//60
    seconds = secs - days*86400 - hours*3600 - minutes*60
    result = ("{0}d ".format(days)) + \
    ("{0}h ".format(hours)) + \
    ("{0}m ".format(minutes)) + \
    ("{0}s".format(seconds))
    return result

# Process each hard state change after comparison with maintenance windows and addition of valid duration to each state's bucket
def process_state_result(state, has_downtime, actual_downtime_dict, found_larger_interval_than_state_duration, time_to_be_added, previous_state, state_end_time, list_result, final_dict, outage_data_dict, downtime_continued):
    state_name = ""
    state_type = ""
    if state == 0:
        state_name = "ok_interval"
    elif state == 1:
        state_name = "warning_interval"
        state_type = "Warning"
    elif state == 2:
        state_name = "critical_interval"
        state_type = "Critical"
    else:
        state_name = "unknown_interval"
        state_type = "Unknown"

    if has_downtime:
        # Processing each downtime for a unique combination of host and service for a given state change record
        for actual_downtime_index in actual_downtime_dict[list_result[0]][list_result[1]]:
            # The current state change is within the boundaries of a maintenance window
            if found_larger_interval_than_state_duration is True:
                break
            # If processing the first maintenance window
            if time_to_be_added == 0:
                # If the current maintenance window ends in past or starts in future w.r.t. state's duration, add the complete state duration to respective state bucket
                if actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] > int(state_end_time) or actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"] < final_dict[list_result[0]][list_result[1]]["last_state_time"]:
                    time_to_be_added = time_to_be_added - final_dict[list_result[0]][list_result[1]]["last_state_time"] + int(state_end_time)
                    # Add an outage record starting from the last maintenance window's end time till state change end
                    if state_name != "ok_interval":
                        outage_data_dict[list_result[0]][list_result[1]].append(str(final_dict[list_result[0]][list_result[1]]["last_state_time"]) + "," + str(state_end_time) + ",Service was in " + state_type + " state for " + str(int(state_end_time - final_dict[list_result[0]][list_result[1]]["last_state_time"])) + " seconds" )
                    final_dict[list_result[0]][list_result[1]]["last_state_time"] = int(state_end_time)
                    final_dict[list_result[0]][list_result[1]]["previous_state"] = previous_state
                    # This is being done only to not add the entry for outage of 0 seconds after the loop exits and checks for downtime_continued = False as the state_end_time and last_state_time are already equal here
                    downtime_continued = True
                else:
                    # If the current maintenance windows starts after the state change and ends before the state change
                    if actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] > final_dict[list_result[0]][list_result[1]]["last_state_time"] and actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"] < int(state_end_time):
                        time_to_be_added = time_to_be_added - final_dict[list_result[0]][list_result[1]]["last_state_time"] + actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] - actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"] + int(state_end_time)
                        # Add an outage record from the start of state record to the start of current maintenance window. The latter part is handled after the loop exits
                        if state_name != "ok_interval":
                            outage_data_dict[list_result[0]][list_result[1]].append(str(final_dict[list_result[0]][list_result[1]]["last_state_time"]) + "," + str(actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"]) + ",Service was in " + state_type + " state for " + str(int(actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] - final_dict[list_result[0]][list_result[1]]["last_state_time"])) + " seconds" )
                        final_dict[list_result[0]][list_result[1]]["last_state_time"] = actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"]
                        final_dict[list_result[0]][list_result[1]]["previous_state"] = previous_state
                        # The downtime does not continue to next state change
                        downtime_continued = False
                    # If the current maintenance window starts before and ends after the current state change
                    elif actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] <= final_dict[list_result[0]][list_result[1]]["last_state_time"] and actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"] >= int(state_end_time):
                        found_larger_interval_than_state_duration = True
                        continue
                    else:
                        # If the current maintenance window starts during the current state and continues to next state
                        if actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] > final_dict[list_result[0]][list_result[1]]["last_state_time"]:
                            time_to_be_added = time_to_be_added - final_dict[list_result[0]][list_result[1]]["last_state_time"] + actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"]
                            # Add the duration from start of the state till the start of current maintenance window
                            if state_name != "ok_interval":
                                outage_data_dict[list_result[0]][list_result[1]].append(str(final_dict[list_result[0]][list_result[1]]["last_state_time"]) + "," + str(actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"]) + ",Service was in " + state_type + " state for " + str(int(actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] - final_dict[list_result[0]][list_result[1]]["last_state_time"])) + " seconds" )
                            final_dict[list_result[0]][list_result[1]]["last_state_time"] = actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"]
                            final_dict[list_result[0]][list_result[1]]["previous_state"] = previous_state
                            # The downtime continues to the next state change
                            downtime_continued = True
                        # If the current maintenance window persists from the previous state and ends during the current state
                        elif actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"] < int(state_end_time):
                            time_to_be_added = time_to_be_added - actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"] + int(state_end_time)
                            final_dict[list_result[0]][list_result[1]]["last_state_time"] = actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"]
                            final_dict[list_result[0]][list_result[1]]["previous_state"] = previous_state
                            # Add the duration from end of the current maintenance window till the end of current state record
                            downtime_continued = False
            # To process all the maintenance windows after the first one, since time has already been added and might need to be deducated based on maintenance windows found in the current record
            else:
                # If the current maintenance window ends in past or starts in future w.r.t. state's duration, do nothing as time has already been added
                if actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] > int(state_end_time) or actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"] < final_dict[list_result[0]][list_result[1]]["last_state_time"]:
                    continue
                else:
                    # If the current maintenance windows starts after the state change and ends before the state change, subtract the maintenance window duration from time already added
                    if actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] > final_dict[list_result[0]][list_result[1]]["last_state_time"] and actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"] < int(state_end_time):
                        time_to_be_added = time_to_be_added + actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] - actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"]
                        # Add an outage record starting from the end of last maintenance window to the start of current maintenance window. The latter part is handled after the loop exits
                        if state_name != "ok_interval":
                            outage_data_dict[list_result[0]][list_result[1]].append(str(final_dict[list_result[0]][list_result[1]]["last_state_time"]) + "," + str(actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"]) + ",Service was in " + state_type + " state for " + str(int(actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] - final_dict[list_result[0]][list_result[1]]["last_state_time"])) + " seconds" )
                        final_dict[list_result[0]][list_result[1]]["last_state_time"] = actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_end_time"]
                        final_dict[list_result[0]][list_result[1]]["previous_state"] = previous_state
                        # The downtime does not continue to next state change
                        downtime_continued = False
                    # If the current maintenance window starts during the current state and continues to next state
                    elif actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] > final_dict[list_result[0]][list_result[1]]["last_state_time"]:
                        time_to_be_added = time_to_be_added - int(state_end_time) + actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"]
                        if state_name != "ok_interval":
                            outage_data_dict[list_result[0]][list_result[1]].append(str(final_dict[list_result[0]][list_result[1]]["last_state_time"]) + "," + str(actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"]) + ",Service was in " + state_type + " state for " + str(int(actual_downtime_dict[list_result[0]][list_result[1]][actual_downtime_index]["actual_start_time"] - final_dict[list_result[0]][list_result[1]]["last_state_time"])) + " seconds" )
                        final_dict[list_result[0]][list_result[1]]["last_state_time"] = actual_downtime_dict[list_result[0]][list_result[1]]["actual_start_time"]
                        final_dict[list_result[0]][list_result[1]]["previous_state"] = previous_state
                        # The downtime continues to the next state change
                        downtime_continued = True
        
        # If the maintenance did not continue to next state, then the last valid time for calculation would the end time of the state change
        if found_larger_interval_than_state_duration is False:
            if downtime_continued is False:
                # Add an outage record starting from the last maintenance window's end time till state change end
                if state_name != "ok_interval":
                    outage_data_dict[list_result[0]][list_result[1]].append(str(final_dict[list_result[0]][list_result[1]]["last_state_time"]) + "," + str(state_end_time) + ",Service was in " + state_type + " state for " + str(int(state_end_time - final_dict[list_result[0]][list_result[1]]["last_state_time"])) + " seconds" )
                final_dict[list_result[0]][list_result[1]]["last_state_time"] = int(state_end_time)
            final_dict[list_result[0]][list_result[1]][state_name] = final_dict[list_result[0]][list_result[1]][state_name] + time_to_be_added
        else:
            final_dict[list_result[0]][list_result[1]]["previous_state"] = previous_state
    else:
        final_dict[list_result[0]][list_result[1]][state_name] = final_dict[list_result[0]][list_result[1]][state_name] - final_dict[list_result[0]][list_result[1]]["last_state_time"] + int(state_end_time)
        if state_name != "ok_interval":
            outage_data_dict[list_result[0]][list_result[1]].append(str(final_dict[list_result[0]][list_result[1]]["last_state_time"]) + "," + str(state_end_time) + ",Service was in " + state_type + " state for " + str(int(state_end_time - final_dict[list_result[0]][list_result[1]]["last_state_time"])) + " seconds" )
        final_dict[list_result[0]][list_result[1]]["last_state_time"] = int(state_end_time)
        final_dict[list_result[0]][list_result[1]]["previous_state"] = previous_state


# Function to calculate state time irrespective of maintenance windows
def process_actual_state_result(final_dict, list_result, actual_outage_data_dict, actual_previous_state, state_end_time):
    # Check the state and add the time to respective duration bucket. Create an outage record if not found in OK.
    if final_dict[list_result[0]][list_result[1]]["actual_previous_state"] == 0:
        final_dict[list_result[0]][list_result[1]]["actual_ok_interval"] = final_dict[list_result[0]][list_result[1]]["actual_ok_interval"] - final_dict[list_result[0]][list_result[1]]["actual_last_state_time"] + int(state_end_time)
    elif final_dict[list_result[0]][list_result[1]]["actual_previous_state"] == 1:
        final_dict[list_result[0]][list_result[1]]["actual_warning_interval"] = final_dict[list_result[0]][list_result[1]]["actual_warning_interval"] - final_dict[list_result[0]][list_result[1]]["actual_last_state_time"] + int(state_end_time)
        actual_outage_data_dict[list_result[0]][list_result[1]].append(str(final_dict[list_result[0]][list_result[1]]["actual_last_state_time"]) + "," + str(state_end_time) + ",Service was in Warning state for " + str(int(state_end_time - final_dict[list_result[0]][list_result[1]]["actual_last_state_time"])) + " seconds" )
    elif final_dict[list_result[0]][list_result[1]]["actual_previous_state"] == 2:
        final_dict[list_result[0]][list_result[1]]["actual_critical_interval"] = final_dict[list_result[0]][list_result[1]]["actual_critical_interval"] - final_dict[list_result[0]][list_result[1]]["actual_last_state_time"] + int(state_end_time)
        actual_outage_data_dict[list_result[0]][list_result[1]].append(str(final_dict[list_result[0]][list_result[1]]["actual_last_state_time"]) + "," + str(state_end_time) + ",Service was in Critical state for " + str(int(state_end_time - final_dict[list_result[0]][list_result[1]]["actual_last_state_time"])) + " seconds" )
    else:
        final_dict[list_result[0]][list_result[1]]["actual_unknown_interval"] = final_dict[list_result[0]][list_result[1]]["actual_unknown_interval"] - final_dict[list_result[0]][list_result[1]]["actual_last_state_time"] + int(state_end_time)
        actual_outage_data_dict[list_result[0]][list_result[1]].append(str(final_dict[list_result[0]][list_result[1]]["actual_last_state_time"]) + "," + str(state_end_time) + ",Service was in Unknown state for " + str(int(state_end_time - final_dict[list_result[0]][list_result[1]]["actual_last_state_time"])) + " seconds" )
    final_dict[list_result[0]][list_result[1]]["actual_last_state_time"] = int(state_end_time)
    final_dict[list_result[0]][list_result[1]]["actual_previous_state"] = actual_previous_state

# Process the services which were unchanged during the report's time window and remove the maintenance window time frame
def process_unchanged_services_with_downtime(state, final_dict, not_found_result, actual_outage_data_dict,  key, service, outage_data_dict):
    state_name = ""
    actual_state_name = ""
    state_type = ""
    if state == 0:
        state_name = "ok_interval"
        actual_state_name = "actual_ok_interval"
    elif state == 1:
        state_name = "warning_interval"
        state_type = "Warning"
        actual_state_name = "actual_warning_interval"
    elif state == 2:
        state_name = "critical_interval"
        state_type = "Critical"
        actual_state_name = "actual_critical_interval"
    else:
        state_name = "unknown_interval"
        state_type = "Unknown"
        actual_state_name = "actual_unknown_interval"

    # Add the entire duration to the respective bucket (unaffected by maintenance windows)
    final_dict[not_found_result[0][0]][not_found_result[0][1]][actual_state_name] = final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]
    # Add the respective outage entry
    if state_name != "ok_interval":
        actual_outage_data_dict[not_found_result[0][0]][not_found_result[0][1]].append(str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]) + "," + str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]) + ",Service was in " + state_type + " state for " + str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]) + " seconds" )
    # If a service for a host has maintenance window(s) scheduled
    if key in actual_downtime_dict.keys():
        if service in actual_downtime_dict[key].keys():
            time_to_be_added = 0
            found_larger_interval_than_state_duration = False
            downtime_continued = False
            # Processing each downtime
            for actual_downtime_index in actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]]:
                # The current state change is within the boundaries of a maintenance window
                if found_larger_interval_than_state_duration is True:
                    break
                # If processing the first maintenance window
                if time_to_be_added == 0:
                    # If the current maintenance window ends in past or starts in future w.r.t. state's duration, add the complete state duration to respective state bucket
                    if actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] > final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"] or actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"] < final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]:
                        time_to_be_added = time_to_be_added - final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] + final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]
                        final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] = final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]
                    else:
                        # If the current maintenance windows starts after the state change and ends before the state change
                        if actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] > final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] and actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"] < final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]:
                            time_to_be_added = time_to_be_added - final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] + actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] - actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"] + final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]
                            # Add an outage record from the start of state record to the start of current maintenance window. The latter part is handled after the loop exits
                            if state_name != "ok_interval":
                                outage_data_dict[not_found_result[0][0]][not_found_result[0][1]].append(str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]) + "," + str(actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"]) + ",Service was in " + state_type + " state for " + str(int(actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"])) + " seconds" )
                            final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] = actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"]
                            # The downtime does not continue to next state change
                            downtime_continued = False
                        # If the current maintenance window starts before and ends after the current state change
                        elif actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] < final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] and actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"] > final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]:
                            found_larger_interval_than_state_duration = True
                            continue
                        else:
                            # If the current maintenance window starts during the current state and continues to next state
                            if actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] > final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]:
                                time_to_be_added = time_to_be_added - final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] + actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"]
                                # Add the duration from start of the state till the start of current maintenance window
                                if state_name != "ok_interval":
                                    outage_data_dict[not_found_result[0][0]][not_found_result[0][1]].append(str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]) + "," + str(actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"]) + ",Service was in " + state_type + " state for " + str(int(actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"])) + " seconds" )
                                final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] = actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"]
                                # The downtime continues to the next state change
                                downtime_continued = True
                            # If the current maintenance window persists from the previous state and ends during the current state
                            elif actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"] < final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]:
                                time_to_be_added = time_to_be_added - actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"] + final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]
                                final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] = actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"]
                                # Add the duration from end of the current maintenance window till the end of current state record
                                downtime_continued = False
                # To process all the maintenance windows after the first one, since time has already been added and might need to be deducated based on maintenance windows found in the current record
                else:
                    # If the current maintenance window ends in past or starts in future w.r.t. state's duration, do nothing as time has already been added
                    if actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] > final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"] or actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"] < final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]:
                        continue
                    else:
                        # If the current maintenance windows starts after the state change and ends before the state change, subtract the maintenance window duration from time already added
                        if actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] > final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] and actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"] < final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]:
                            time_to_be_added = time_to_be_added + actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] - actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"]
                            # Add an outage record starting from the end of last maintenance window to the start of current maintenance window. The latter part is handled after the loop exits
                            if state_name != "ok_interval":
                                outage_data_dict[not_found_result[0][0]][not_found_result[0][1]].append(str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]) + "," + str(actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"]) + ",Service was in " + state_type + " state for " + str(int(actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"])) + " seconds" )
                            final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] = actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_end_time"]
                            # The downtime does not continue to next state change
                            downtime_continued = False
                        # If the current maintenance window starts during the current state and continues to next state
                        elif actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"] > final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]:
                            time_to_be_added = time_to_be_added - final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"] + actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]][actual_downtime_index]["actual_start_time"]
                            final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] = actual_downtime_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_start_time"]
                            # The downtime continues to the next state change
                            downtime_continued = True
            
            # If the maintenance did not continue to next state, then the last valid time for calculation would the end time of the state change
            if downtime_continued is False:
                final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"] = final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]
                # Add an outage record starting from the last maintenance window's end time till state change end
                if state_name != "ok_interval":
                    outage_data_dict[not_found_result[0][0]][not_found_result[0][1]].append(str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]) + "," + str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]) + ",Service was in " + state_type + " state for " + str(int(final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"])) + " seconds" )
            final_dict[not_found_result[0][0]][not_found_result[0][1]][state_name] = final_dict[not_found_result[0][0]][not_found_result[0][1]][state_name] + time_to_be_added
            # Return 1 to inform that nothing else needs to be done with the current service for the host
            return 1
    # If a service for a host has no maintenance window scheduled
    final_dict[not_found_result[0][0]][not_found_result[0][1]][state_name] = final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]
    if state_name != "ok_interval":
        outage_data_dict[not_found_result[0][0]][not_found_result[0][1]].append(str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]) + "," + str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"]) + ",Service was in " + state_type + " state for " + str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["last_state_time"]) + " seconds" )
    return 0

# Prepare arguements for parsing

parser = argparse.ArgumentParser()
parser.add_argument('-s', '--start-time', help='Starting time of the report', required=True)
parser.add_argument('-e', '--end-time', help='Ending time of the report', required=True)

pattern = '%Y.%m.%d %H:%M:%S'

args = parser.parse_args()

start_epoch_time = int(time.mktime(time.strptime(args.start_time, pattern)))
end_epoch_time = int(time.mktime(time.strptime(args.end_time, pattern)))

# Construct connection string

try:
    conn = mysql.connector.connect(user="username", password="password", host="mysql host address", database="statusengine")
    print("Connection established")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with the user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursor = conn.cursor()

    # Get all the scheduled downtimes overlapping the report window
    cursor.execute("SELECT hostname, service_description, author_name, comment_data, scheduled_start_time, scheduled_end_time, actual_start_time, actual_end_time, internal_downtime_id from statusengine_service_downtimehistory where scheduled_start_time <= %s and scheduled_end_time >= %s and service_description LIKE '%My Service Name%'",(end_epoch_time,start_epoch_time,))
    list_scheduled_downtime_results = list(cursor.fetchall())

    list_actual_downtime_results = list_scheduled_downtime_results

    # Dictionary to store the list of maintenance windows after processing for overlaps
    actual_downtime_dict = dict()
    # Dictionary to store the list of maintenance windows to output in CSV
    scheduled_downtime_dict = dict()
    # Dictionary to store the list of maintenance windows where the actual end time is modified in case the maintenance is still going on
    interpreted_downtime_dict = dict()

    for actual_downtime_result in list_actual_downtime_results:
        index = 0
        if actual_downtime_result[0] in interpreted_downtime_dict.keys():
            downtime_dict = dict()
            downtime_dict["author_name"] = actual_downtime_result[2]
            downtime_dict["comment_data"] = actual_downtime_result[3]
            downtime_dict["scheduled_start_time"] = actual_downtime_result[4]
            downtime_dict["scheduled_end_time"] = actual_downtime_result[5]
            downtime_dict["actual_start_time"] = actual_downtime_result[6]
            # Setting the end_time to end of the report time in case the maintenance is still going on
            if actual_downtime_result[7] == 0:
                downtime_dict["actual_end_time"] = end_epoch_time
            else:
                downtime_dict["actual_end_time"] = actual_downtime_result[7]
            if actual_downtime_result[1] in interpreted_downtime_dict[actual_downtime_result[0]].keys():
                interpreted_downtime_dict[actual_downtime_result[0]][actual_downtime_result[1]][actual_downtime_result[8]] = downtime_dict
            else:
                temp_dict = dict()
                temp_dict[actual_downtime_result[8]] = downtime_dict
                interpreted_downtime_dict[actual_downtime_result[0]][actual_downtime_result[1]]= temp_dict
        else:
            host_dict = dict()
            downtime_dict = dict()
            downtime_instance_dict = dict()
            downtime_dict["author_name"] = actual_downtime_result[2]
            downtime_dict["comment_data"] = actual_downtime_result[3]
            downtime_dict["scheduled_start_time"] = actual_downtime_result[4]
            downtime_dict["scheduled_end_time"] = actual_downtime_result[5]
            downtime_dict["actual_start_time"] = actual_downtime_result[6]
            # Setting the end_time to end of the report time in case the maintenance is still going on
            if actual_downtime_result[7] == 0:
                downtime_dict["actual_end_time"] = end_epoch_time
            else:
                downtime_dict["actual_end_time"] = actual_downtime_result[7]
            downtime_instance_dict[actual_downtime_result[8]] = downtime_dict
            host_dict[actual_downtime_result[1]] = downtime_instance_dict
            interpreted_downtime_dict[actual_downtime_result[0]] = host_dict

    for scheduled_downtime_result in list_scheduled_downtime_results:
        if scheduled_downtime_result[0] in scheduled_downtime_dict.keys():
            downtime_dict = dict()
            downtime_dict["author_name"] = scheduled_downtime_result[2]
            downtime_dict["comment_data"] = scheduled_downtime_result[3]
            downtime_dict["scheduled_start_time"] = scheduled_downtime_result[4]
            downtime_dict["scheduled_end_time"] = scheduled_downtime_result[5]
            downtime_dict["actual_start_time"] = scheduled_downtime_result[6]
            downtime_dict["actual_end_time"] = scheduled_downtime_result[7]
            if scheduled_downtime_result[1] in scheduled_downtime_dict[scheduled_downtime_result[0]].keys():
                scheduled_downtime_dict[scheduled_downtime_result[0]][scheduled_downtime_result[1]][scheduled_downtime_result[8]] = downtime_dict
            else:
                temp_dict = dict()
                temp_dict[scheduled_downtime_result[8]] = downtime_dict
                scheduled_downtime_dict[scheduled_downtime_result[0]][scheduled_downtime_result[1]]= temp_dict
        else:
            host_dict = dict()
            downtime_dict = dict()
            downtime_instance_dict = dict()
            downtime_dict["author_name"] = scheduled_downtime_result[2]
            downtime_dict["comment_data"] = scheduled_downtime_result[3]
            downtime_dict["scheduled_start_time"] = scheduled_downtime_result[4]
            downtime_dict["scheduled_end_time"] = scheduled_downtime_result[5]
            downtime_dict["actual_start_time"] = scheduled_downtime_result[6]
            downtime_dict["actual_end_time"] = scheduled_downtime_result[7]
            downtime_instance_dict[scheduled_downtime_result[8]] = downtime_dict
            host_dict[scheduled_downtime_result[1]] = downtime_instance_dict
            scheduled_downtime_dict[scheduled_downtime_result[0]] = host_dict

    # Processibg the maintenance windows to get rid of the overlaps
    for host in interpreted_downtime_dict:
        for service in interpreted_downtime_dict[host]:

            comparative_time_list = list()
            for internal_downtime_id in interpreted_downtime_dict[host][service]:
                if not comparative_time_list:
                    time_tuple = (interpreted_downtime_dict[host][service][internal_downtime_id]["actual_start_time"], interpreted_downtime_dict[host][service][internal_downtime_id]["actual_end_time"])
                    comparative_time_list.append(time_tuple)
                else:
                    remove_list = list()
                    index = -1
                    new_comparative_time_list = list()
                    for comparative_time in comparative_time_list:
                        index += 1
                        if interpreted_downtime_dict[host][service][internal_downtime_id]["actual_start_time"] < comparative_time[0] and interpreted_downtime_dict[host][service][internal_downtime_id]["actual_end_time"] > comparative_time[1]:
                            remove_list.append(index)
                            time_tuple = (interpreted_downtime_dict[host][service][internal_downtime_id]["actual_start_time"], interpreted_downtime_dict[host][service][internal_downtime_id]["actual_end_time"])
                            new_comparative_time_list.append(time_tuple)
                        elif interpreted_downtime_dict[host][service][internal_downtime_id]["actual_start_time"] >= comparative_time[0] and interpreted_downtime_dict[host][service][internal_downtime_id]["actual_end_time"] <= comparative_time[1]:
                            continue
                        elif interpreted_downtime_dict[host][service][internal_downtime_id]["actual_start_time"] < comparative_time[0]:
                            if interpreted_downtime_dict[host][service][internal_downtime_id]["actual_end_time"] < comparative_time[0] - 1:
                                time_tuple = (interpreted_downtime_dict[host][service][internal_downtime_id]["actual_start_time"], interpreted_downtime_dict[host][service][internal_downtime_id]["actual_end_time"])
                                new_comparative_time_list.append(time_tuple)
                            else:
                                time_tuple = (interpreted_downtime_dict[host][service][internal_downtime_id]["actual_start_time"], comparative_time[1])
                                new_comparative_time_list.append(time_tuple)
                                remove_list.append(index)
                        elif interpreted_downtime_dict[host][service][internal_downtime_id]["actual_end_time"] > comparative_time[1]:
                            if interpreted_downtime_dict[host][service][internal_downtime_id]["actual_end_time"] > comparative_time[1] - 1:
                                time_tuple = (interpreted_downtime_dict[host][service][internal_downtime_id]["actual_start_time"], interpreted_downtime_dict[host][service][internal_downtime_id]["actual_end_time"])
                                new_comparative_time_list.append(time_tuple)
                            else:
                                time_tuple = (comparative_time[0], interpreted_downtime_dict[host][service][internal_downtime_id]["actual_end_time"])
                                new_comparative_time_list.append(time_tuple)
                                remove_list.append(index)
                    index2 = 0
                    for remove_index in remove_list:
                        comparative_time_list.pop(remove_index - index2)
                        index2 += 1

                    comparative_time_list.extend(new_comparative_time_list)

            index_dict = dict()
            for comparative_time in comparative_time_list:
                index = 0
                downtime_dict = dict()

                downtime_dict["actual_start_time"] = comparative_time[0]
                downtime_dict["actual_end_time"] = comparative_time[1]
                index_dict[index] = downtime_dict
                index += 1

            if host in actual_downtime_dict.keys():
                actual_downtime_dict[host][service] = index_dict
            else:
                service_dict = dict()
                service_dict[service] = index_dict
                actual_downtime_dict[host] = service_dict
    
    not_found_dict= dict()

    for host_key in actual_downtime_dict:
        for service_key in actual_downtime_dict[host_key]:
            for internal_downtime_id in actual_downtime_dict[host_key][service_key]:
                if actual_downtime_dict[host_key][service_key][internal_downtime_id]["actual_start_time"] <= start_epoch_time and actual_downtime_dict[host_key][service_key][internal_downtime_id]["actual_end_time"] >= end_epoch_time:
                    if host_key in not_found_dict.keys():
                        not_found_dict[host_key].append(service_key)
                    else:
                        temp_list = list()
                        temp_list.append(service_key)
                        not_found_dict[host_key] = temp_list
    
    csv_schedule_file_name = "Service-Planned-Downtime.csv"
    csv_schedule_file = open(csv_schedule_file_name, "w")
    csv_schedule_file.write("Hostname,Service Name,Author,Comment,Scheduled Start Time,Scheduled End Time, Actual Start Time, Actual End Time\n")

    for host_key in scheduled_downtime_dict:
        for service_key in scheduled_downtime_dict[host_key]:
            for internal_downtime_id in scheduled_downtime_dict[host_key][service_key]:
                csv_schedule_file.write(host_key + "," + service_key + "," + str(scheduled_downtime_dict[host_key][service_key][internal_downtime_id]["author_name"]) + "," + str(scheduled_downtime_dict[host_key][service_key][internal_downtime_id]["comment_data"].encode('ascii', 'ignore').decode('ascii')) + "," + str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(scheduled_downtime_dict[host_key][service_key][internal_downtime_id]["scheduled_start_time"]))) + "," + str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(scheduled_downtime_dict[host_key][service_key][internal_downtime_id]["scheduled_end_time"]))) + "," + str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(scheduled_downtime_dict[host_key][service_key][internal_downtime_id]["actual_start_time"]))) + "," + str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(scheduled_downtime_dict[host_key][service_key][internal_downtime_id]["actual_end_time"]))) + "\n")

    csv_schedule_file.close()

    #cursor.execute("SELECT distinct(hostname),service_description from statusengine_servicechecks where start_time >= %s and end_time <= %s and service_description LIKE '%My Service Name%' order by hostname, service_description;",(start_epoch_time,end_epoch_time,))
    cursor.execute("select hostname, service_description from service_monitor_time_range where service_description LIKE '%My Service Name%' and max_start_time >= %s and min_start_time <= %s;",(start_epoch_time,end_epoch_time,))
    list_results = list(cursor.fetchall())

    cursor.execute("SELECT hostname, service_description, state, state_time, last_state, last_hard_state from statusengine_service_statehistory where state_time >= %s and state_time <= %s and is_hardstate = 1 and service_description LIKE '%My Service Name%' ORDER BY state_time;",(start_epoch_time,end_epoch_time,))
    state_results = list(cursor.fetchall())

    cursor.execute("SELECT hostname, service_description, min_start_time FROM service_monitor_time_range where service_description LIKE '%My Service Name%';")
    first_check_results = list(cursor.fetchall())

    cursor.execute("SELECT hostname, service_description, max_start_time FROM service_monitor_time_range where service_description LIKE '%My Service Name%';")
    last_check_results = list(cursor.fetchall())

    
    final_dict= dict()
    outage_data_dict = dict()
    actual_outage_data_dict = dict()
    first_check_dict = dict()
    last_check_dict = dict()

    for first_check in first_check_results:
        first_check_stats_dict = dict()
        first_check_stats_dict["start_time"] = first_check[2]

        if first_check[0] in first_check_dict.keys():
            first_check_dict[first_check[0]][first_check[1]] = first_check_stats_dict
        else:
            host_dict = dict()
            host_dict[first_check[1]] = first_check_stats_dict
            first_check_dict[first_check[0]] = host_dict

    for last_check in last_check_results:
        last_check_stats_dict = dict()
        last_check_stats_dict["start_time"] = last_check[2]

        if last_check[0] in last_check_dict.keys():
            last_check_dict[last_check[0]][last_check[1]] = last_check_stats_dict
        else:
            host_dict = dict()
            host_dict[last_check[1]] = last_check_stats_dict
            last_check_dict[last_check[0]] = host_dict


    for list_result in list_results:
        found_flag = False
        statistics_dict = dict()
        statistics_dict["ok_interval"] = 0
        statistics_dict["warning_interval"] = 0
        statistics_dict["critical_interval"] = 0
        statistics_dict["unknown_interval"] = 0
        statistics_dict["actual_ok_interval"] = 0
        statistics_dict["actual_warning_interval"] = 0
        statistics_dict["actual_critical_interval"] = 0
        statistics_dict["actual_unknown_interval"] = 0
        statistics_dict["skip_for_actual"] = False
        if first_check_dict[list_result[0]][list_result[1]]["start_time"] < start_epoch_time:
            statistics_dict["last_state_time"] = start_epoch_time
            statistics_dict["actual_last_state_time"] = start_epoch_time
        else:
            statistics_dict["last_state_time"] = first_check_dict[list_result[0]][list_result[1]]["start_time"]
            statistics_dict["actual_last_state_time"] = first_check_dict[list_result[0]][list_result[1]]["start_time"]
        
        if last_check_dict[list_result[0]][list_result[1]]["start_time"] >= end_epoch_time:
            statistics_dict["final_state_time"] = end_epoch_time
            statistics_dict["actual_final_state_time"] = end_epoch_time
        else:
            statistics_dict["final_state_time"] = last_check_dict[list_result[0]][list_result[1]]["start_time"]
            statistics_dict["actual_final_state_time"] = last_check_dict[list_result[0]][list_result[1]]["start_time"]
        
        if list_result[0] in final_dict.keys():
            final_dict[list_result[0]][list_result[1]] = statistics_dict
        else:
            host_dict = dict()
            host_dict[list_result[1]] = statistics_dict
            final_dict[list_result[0]] = host_dict

        if list_result[0] in outage_data_dict.keys():
            temp_list = list()
            outage_data_dict[list_result[0]][list_result[1]] = temp_list
        else:
            temp_list = list()
            host_dict = dict()
            host_dict[list_result[1]] = temp_list
            outage_data_dict[list_result[0]] = host_dict

        if list_result[0] in actual_outage_data_dict.keys():
            temp_list = list()
            actual_outage_data_dict[list_result[0]][list_result[1]] = temp_list
        else:
            temp_list = list()
            host_dict = dict()
            host_dict[list_result[1]] = temp_list
            actual_outage_data_dict[list_result[0]] = host_dict

        has_downtime = False

        if list_result[0] in actual_downtime_dict.keys():
            if list_result[1] in actual_downtime_dict[list_result[0]].keys():
                has_downtime = True

        for state_result in state_results:
            if list_result[0] == state_result[0] and list_result[1] == state_result[1]:
                if found_flag is False:
                    if state_result[2] == state_result[5] and state_result[2] == state_result[4]:
                        cursor.execute("SELECT state, last_state, last_hard_state from statusengine_service_statehistory where state_time < %s and hostname = %s and service_description = %s ORDER BY state_time DESC LIMIT 1;",(state_result[3],state_result[0],state_result[1],))
                        temp_state_results = list(cursor.fetchall())
                        temp_state_result = temp_state_results[0]
                        if temp_state_result[0] != temp_state_result[2]:
                            final_dict[list_result[0]][list_result[1]]["previous_state"] = temp_state_result[2]
                            final_dict[list_result[0]][list_result[1]]["actual_previous_state"] = temp_state_result[2]
                        else:
                            final_dict[list_result[0]][list_result[1]]["previous_state"] = temp_state_result[1]
                            final_dict[list_result[0]][list_result[1]]["actual_previous_state"] = temp_state_result[1]
                    else:
                        if state_result[2] != state_result[5]:
                            final_dict[list_result[0]][list_result[1]]["previous_state"] = state_result[5]
                            final_dict[list_result[0]][list_result[1]]["actual_previous_state"] = state_result[5]
                        else:
                            final_dict[list_result[0]][list_result[1]]["previous_state"] = state_result[4]
                            final_dict[list_result[0]][list_result[1]]["actual_previous_state"] = state_result[4]
                found_flag = True
                
                process_actual_state_result(final_dict, list_result, actual_outage_data_dict, state_result[2], state_result[3])
                
                time_to_be_added = 0
                found_larger_interval_than_state_duration = False
                downtime_continued = False

                process_state_result(final_dict[list_result[0]][list_result[1]]["previous_state"], has_downtime, actual_downtime_dict, found_larger_interval_than_state_duration, time_to_be_added, state_result[2], state_result[3], list_result, final_dict, outage_data_dict, downtime_continued)

        if found_flag is True:
            process_actual_state_result(final_dict, list_result, actual_outage_data_dict, 4, final_dict[list_result[0]][list_result[1]]["actual_final_state_time"])

            time_to_be_added = 0
            found_larger_interval_than_state_duration = False
            downtime_continued = False

            process_state_result(final_dict[list_result[0]][list_result[1]]["previous_state"], has_downtime, actual_downtime_dict, found_larger_interval_than_state_duration, time_to_be_added, 4, final_dict[list_result[0]][list_result[1]]["final_state_time"], list_result, final_dict, outage_data_dict, downtime_continued)

            if final_dict[list_result[0]][list_result[1]]["ok_interval"] == 0 and final_dict[list_result[0]][list_result[1]]["warning_interval"] == 0 and final_dict[list_result[0]][list_result[1]]["critical_interval"] == 0 and final_dict[list_result[0]][list_result[1]]["unknown_interval"] == 0:
                final_dict[list_result[0]][list_result[1]]["skip_for_actual"] = True
                if list_result[0] in not_found_dict.keys():
                    not_found_dict[list_result[0]].append(list_result[1])
                else:
                    temp_list = list()
                    temp_list.append(list_result[1])
                    not_found_dict[list_result[0]] = temp_list
        else:
            if list_result[0] in not_found_dict.keys():
                if list_result[1] not in not_found_dict[list_result[0]]:
                    not_found_dict[list_result[0]].append(list_result[1])
            else:
                temp_list = list()
                temp_list.append(list_result[1])
                not_found_dict[list_result[0]] = temp_list

    ## Building query to get the states of not found entries.

    for key in not_found_dict:
        for service in not_found_dict[key]:
            if key in actual_downtime_dict.keys():
                if service in actual_downtime_dict[key].keys():
                    for internal_downtime_id in actual_downtime_dict[key][service]:
                        if actual_downtime_dict[key][service][internal_downtime_id]["actual_start_time"] <= final_dict[key][service]["last_state_time"] and actual_downtime_dict[key][service][internal_downtime_id]["actual_end_time"] >= final_dict[key][service]["final_state_time"]:
                            final_dict[key][service]["ok_interval"] = "Under Downtime"
                            final_dict[key][service]["warning_interval"] = "Under Downtime"
                            final_dict[key][service]["critical_interval"] = "Under Downtime"
                            final_dict[key][service]["unknown_interval"] = "Under Downtime"
                            if final_dict[key][service]["skip_for_actual"] is False:
                                if final_dict[key][service]["actual_ok_interval"] != 0 or final_dict[key][service]["actual_warning_interval"] != 0 or final_dict[key][service]["actual_critical_interval"] != 0 or final_dict[key][service]["actual_unknown_interval"] != 0:
                                    continue
                                cursor.execute("SELECT hostname, service_description, last_hard_state from statusengine_service_statehistory where hostname = %s and service_description = %s and state_time >= %s and state_time <= %s ORDER BY state_time LIMIT 1;",(key,service,start_epoch_time,end_epoch_time,))
                                not_found_result = list(cursor.fetchall())
                                if not not_found_result:
                                    final_dict[key][service]["actual_ok_interval"] = "NULL"
                                    final_dict[key][service]["actual_warning_interval"] = "NULL"
                                    final_dict[key][service]["actual_critical_interval"] = "NULL"
                                    final_dict[key][service]["actual_unknown_interval"] = "NULL"
                                else:
                                    if not_found_result[0][2] == 0:
                                        final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_ok_interval"] = final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_last_state_time"]
                                    elif not_found_result[0][2] == 1:
                                        final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_warning_interval"] = inal_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_last_state_time"]
                                        actual_outage_data_dict[not_found_result[0][0]][not_found_result[0][1]].append(str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_last_state_time"]) + "," + str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_final_state_time"]) + ",Service was in Warning state for " + str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_last_state_time"]) + " seconds" )
                                    elif not_found_result[0][2] == 2:
                                        final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_critical_interval"] = inal_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_last_state_time"]
                                        actual_outage_data_dict[not_found_result[0][0]][not_found_result[0][1]].append(str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_last_state_time"]) + "," + str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_final_state_time"]) + ",Service was in Critical state for " + str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_last_state_time"]) + " seconds" )
                                    else:
                                        final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_unknown_interval"] = inal_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_last_state_time"]
                                        actual_outage_data_dict[not_found_result[0][0]][not_found_result[0][1]].append(str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_last_state_time"]) + "," + str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_final_state_time"]) + ",Service was in Unknown state for " + str(final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_final_state_time"] - final_dict[not_found_result[0][0]][not_found_result[0][1]]["actual_last_state_time"]) + " seconds" )
                                continue

            if final_dict[key][service]["actual_ok_interval"] != 0 or final_dict[key][service]["actual_warning_interval"] != 0 or final_dict[key][service]["actual_critical_interval"] != 0 or final_dict[key][service]["actual_unknown_interval"] != 0:
                continue

            cursor.execute("SELECT hostname, service_description, last_hard_state from statusengine_service_statehistory where hostname = %s and service_description = %s and state_time >= %s and state_time <= %s ORDER BY state_time LIMIT 1;",(key,service,start_epoch_time,end_epoch_time,))
            not_found_result_service_state_history = list(cursor.fetchall())
            cursor.execute("select hostname, service_description, state from statusengine_servicechecks where hostname = %s and service_description = %s and start_time >= %s ORDER BY start_time LIMIT 1;",(key,service,start_epoch_time,))
            not_found_result_service_checks = list(cursor.fetchall())

            not_found_result = list()

            if not_found_result_service_state_history:
                not_found_result = not_found_result_service_state_history
            else:
                not_found_result = not_found_result_service_checks

            if not not_found_result:
                cursor.execute("SELECT hostname, service_description, last_hard_state from statusengine_service_statehistory where hostname = %s and service_description = %s and state_time >= %s and state_time <= %s ORDER BY state_time LIMIT 1;",(key,service,start_epoch_time,end_epoch_time,))
                final_dict[key][service]["ok_interval"] = "NULL"
                final_dict[key][service]["warning_interval"] = "NULL"
                final_dict[key][service]["critical_interval"] = "NULL"
                final_dict[key][service]["unknown_interval"] = "NULL"
                final_dict[key][service]["actual_ok_interval"] = "NULL"
                final_dict[key][service]["actual_warning_interval"] = "NULL"
                final_dict[key][service]["actual_critical_interval"] = "NULL"
                final_dict[key][service]["actual_unknown_interval"] = "NULL"
                continue
            result = process_unchanged_services_with_downtime(not_found_result[0][2], final_dict, not_found_result, actual_outage_data_dict, key, service, outage_data_dict)
            if result == 1:
                continue

    csv_file_name = "Service-Availability.csv"
    csv_file = open(csv_file_name, "w")
    csv_file.write(",,Contractual,,,,,,,,Actual,,,,,,,,\nHostname,Service,Ok,Warning,Critical,Unknown,Ok%,Warning%,Critical%,Unknown%,Ok,Warning,Critical,Unknown,Ok%,Warning%,Critical%,Unknown%\n")

    for host_key in final_dict:
        for service_key in final_dict[host_key]:
            print "Processing " + host_key + " " + service_key
            print final_dict[host_key][service_key]
            if str(final_dict[host_key][service_key]["ok_interval"]) != "NULL" and str(final_dict[host_key][service_key]["ok_interval"]) != "Under Downtime":
                final_dict[host_key][service_key]["ok_interval_human"] = secondsToText(final_dict[host_key][service_key]["ok_interval"])
                final_dict[host_key][service_key]["warning_interval_human"] = secondsToText(final_dict[host_key][service_key]["warning_interval"])
                final_dict[host_key][service_key]["critical_interval_human"] = secondsToText(final_dict[host_key][service_key]["critical_interval"])
                final_dict[host_key][service_key]["unknown_interval_human"] = secondsToText(final_dict[host_key][service_key]["unknown_interval"])
                total = final_dict[host_key][service_key]["ok_interval"] + final_dict[host_key][service_key]["warning_interval"] + final_dict[host_key][service_key]["critical_interval"] + final_dict[host_key][service_key]["unknown_interval"]
                final_dict[host_key][service_key]["ok_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["ok_interval"])*100/float(total))
                final_dict[host_key][service_key]["warning_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["warning_interval"])*100/float(total))
                final_dict[host_key][service_key]["critical_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["critical_interval"])*100/float(total))
                final_dict[host_key][service_key]["unknown_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["unknown_interval"])*100/float(total))
                final_dict[host_key][service_key]["actual_ok_interval_human"] = secondsToText(final_dict[host_key][service_key]["actual_ok_interval"])
                final_dict[host_key][service_key]["actual_warning_interval_human"] = secondsToText(final_dict[host_key][service_key]["actual_warning_interval"])
                final_dict[host_key][service_key]["actual_critical_interval_human"] = secondsToText(final_dict[host_key][service_key]["actual_critical_interval"])
                final_dict[host_key][service_key]["actual_unknown_interval_human"] = secondsToText(final_dict[host_key][service_key]["actual_unknown_interval"])
                actual_total = final_dict[host_key][service_key]["actual_ok_interval"] + final_dict[host_key][service_key]["actual_warning_interval"] + final_dict[host_key][service_key]["actual_critical_interval"] + final_dict[host_key][service_key]["actual_unknown_interval"]
                final_dict[host_key][service_key]["actual_ok_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["actual_ok_interval"])*100/float(actual_total))
                final_dict[host_key][service_key]["actual_warning_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["actual_warning_interval"])*100/float(actual_total))
                final_dict[host_key][service_key]["actual_critical_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["actual_critical_interval"])*100/float(actual_total))
                final_dict[host_key][service_key]["actual_unknown_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["actual_unknown_interval"])*100/float(actual_total))
            else:
                final_dict[host_key][service_key]["ok_interval_human"] = str(final_dict[host_key][service_key]["ok_interval"])
                final_dict[host_key][service_key]["warning_interval_human"] = str(final_dict[host_key][service_key]["ok_interval"])
                final_dict[host_key][service_key]["critical_interval_human"] = str(final_dict[host_key][service_key]["ok_interval"])
                final_dict[host_key][service_key]["unknown_interval_human"] = str(final_dict[host_key][service_key]["ok_interval"])
                final_dict[host_key][service_key]["ok_interval_percentage"] = str(final_dict[host_key][service_key]["ok_interval"])
                final_dict[host_key][service_key]["warning_interval_percentage"] = str(final_dict[host_key][service_key]["ok_interval"])
                final_dict[host_key][service_key]["critical_interval_percentage"] = str(final_dict[host_key][service_key]["ok_interval"])
                final_dict[host_key][service_key]["unknown_interval_percentage"] = str(final_dict[host_key][service_key]["ok_interval"])
                if str(final_dict[host_key][service_key]["actual_ok_interval"]) != "NULL":
                    final_dict[host_key][service_key]["actual_ok_interval_human"] = secondsToText(final_dict[host_key][service_key]["actual_ok_interval"])
                    final_dict[host_key][service_key]["actual_warning_interval_human"] = secondsToText(final_dict[host_key][service_key]["actual_warning_interval"])
                    final_dict[host_key][service_key]["actual_critical_interval_human"] = secondsToText(final_dict[host_key][service_key]["actual_critical_interval"])
                    final_dict[host_key][service_key]["actual_unknown_interval_human"] = secondsToText(final_dict[host_key][service_key]["actual_unknown_interval"])
                    actual_total = final_dict[host_key][service_key]["actual_ok_interval"] + final_dict[host_key][service_key]["actual_warning_interval"] + final_dict[host_key][service_key]["actual_critical_interval"] + final_dict[host_key][service_key]["actual_unknown_interval"]
                    final_dict[host_key][service_key]["actual_ok_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["actual_ok_interval"])*100/float(actual_total))
                    final_dict[host_key][service_key]["actual_warning_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["actual_warning_interval"])*100/float(actual_total))
                    final_dict[host_key][service_key]["actual_critical_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["actual_critical_interval"])*100/float(actual_total))
                    final_dict[host_key][service_key]["actual_unknown_interval_percentage"] = "{0:.10f}".format(int(final_dict[host_key][service_key]["actual_unknown_interval"])*100/float(actual_total))
                else:
                    final_dict[host_key][service_key]["actual_ok_interval_human"] = "NULL"
                    final_dict[host_key][service_key]["actual_warning_interval_human"] = "NULL"
                    final_dict[host_key][service_key]["actual_critical_interval_human"] = "NULL"
                    final_dict[host_key][service_key]["actual_unknown_interval_human"] = "NULL"
                    final_dict[host_key][service_key]["actual_ok_interval_percentage"] = "NULL"
                    final_dict[host_key][service_key]["actual_warning_interval_percentage"] = "NULL"
                    final_dict[host_key][service_key]["actual_critical_interval_percentage"] = "NULL"
                    final_dict[host_key][service_key]["actual_unknown_interval_percentage"] = "NULL"


            csv_file.write(host_key + "," + service_key + "," + str(final_dict[host_key][service_key]["ok_interval_human"]) + "," + str(final_dict[host_key][service_key]["warning_interval_human"]) + "," + str(final_dict[host_key][service_key]["critical_interval_human"]) + "," + str(final_dict[host_key][service_key]["unknown_interval_human"]) + "," + str(final_dict[host_key][service_key]["ok_interval_percentage"]) + "," + str(final_dict[host_key][service_key]["warning_interval_percentage"]) + "," + str(final_dict[host_key][service_key]["critical_interval_percentage"]) + "," + str(final_dict[host_key][service_key]["unknown_interval_percentage"]) + "," + str(final_dict[host_key][service_key]["actual_ok_interval_human"]) + "," + str(final_dict[host_key][service_key]["actual_warning_interval_human"]) + "," + str(final_dict[host_key][service_key]["actual_critical_interval_human"]) + "," + str(final_dict[host_key][service_key]["actual_unknown_interval_human"]) + "," + str(final_dict[host_key][service_key]["actual_ok_interval_percentage"]) + "," + str(final_dict[host_key][service_key]["actual_warning_interval_percentage"]) + "," + str(final_dict[host_key][service_key]["actual_critical_interval_percentage"]) + "," + str(final_dict[host_key][service_key]["actual_unknown_interval_percentage"]) + "\n")

    csv_file.close()

    csv_outage_file_name = "Service-Unplanned-Outage.csv"
    csv_outage_file = open(csv_outage_file_name, "w")
    csv_outage_file.write("Hostname,Service Name,Start Time (UTC),End Time (UTC),Message\n")

    for host_key in outage_data_dict:
        for service_key in outage_data_dict[host_key]:
            if final_dict[host_key][service_key]:
                for outage in outage_data_dict[host_key][service_key]:
                    utc_start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(outage.split(',')[0])))
                    utc_end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(outage.split(',')[1])))
                    csv_outage_file.write(host_key + "," + service_key + "," + str(utc_start_time) + "," + str(utc_end_time) + "," + outage.split(',')[2] + "\n")
    csv_outage_file.close()

    csv_actual_outage_file_name = "Service-Overall-Outage.csv"
    csv_actual_outage_file = open(csv_actual_outage_file_name, "w")
    csv_actual_outage_file.write("Hostname,Service Name,Start Time (UTC),End Time (UTC),Message\n")

    for host_key in outage_data_dict:
        for service_key in outage_data_dict[host_key]:
            if final_dict[host_key][service_key]:
                for actual_outage in actual_outage_data_dict[host_key][service_key]:
                    utc_start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(actual_outage.split(',')[0])))
                    utc_end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(actual_outage.split(',')[1])))
                    csv_actual_outage_file.write(host_key + "," + service_key + "," + str(utc_start_time) + "," + str(utc_end_time) + "," + actual_outage.split(',')[2] + "\n")
    csv_actual_outage_file.close()

    conn.close()
