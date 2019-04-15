# naemon-availability-report

First, create a Mysql view in statusengine DB using the below command

```CREATE VIEW service_monitor_time_range AS select hostname, service_description, min(start_time) AS min_start_time,max(start_time) AS max_start_time from statusengine_servicechecks group by hostname, service_description```

It will spit out 4 CSV files.

* Service Availability: This sheet contains an overview of the duration the given service was in each state as well as the percentage calculation for the same. It comprises of 2 parts:

  * Contractual: The contractual data does not take the state changes and state durations into calculation during the maintenance windows. In simple terms, if a service is under maintenance for 1 hour and the report is generated for that day, the total time considered for all states would be 23 hours.

  * Actual: The actual data does not care whether a service was in maintenance or not. It gives the data for the entire duration which gives the state of the service, its duration and its percentage duration, without taking into consideration whether the service was in maintenance or not.

*	Service Planned Downtime: This workbook contains the list of maintenance windows for the service, including the comments, start time and end time of every maintenance window for that service.

*	Service Unplanned Outage: This workbook contains entries for each of the non-OK state for the service along with when did the non-OK state start and end, based in Contractual concept i.e. does not contain the entries when the service was under maintenance.

*	Service Overall Outage: This workbook contains entries for each of the non-OK state for the service along with when did the non-OK state start and end, based in Actual concept i.e. contains the entries of non-OK states even when service was under maintenance.

# Tables and Views used:

* statusengine_service_downtimehistory (Table): Contains the information of all the maintenance windows
* statusengine_service_downtimehistory (View): Contains the information about timestamp when the service was placed under monitoring and the timestamp of the last check performed.
* statusengine_service_statehistory (Table): Contains the information about all the state changes of the services
* statusengine_servicechecks (Table): Contains the information about every single check ever performed.
