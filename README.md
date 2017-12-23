# Vehicle Telematics

In this project we consider that each vehicle reports a position event every 30 seconds with the following format: Time, VID, Spd, XWay, Lane, Dir, Seg, Pos.

* __Time__: A timestamp (integer) in seconds identifying the time at which the position event was emitted.
* __VID__: An integer that identifies the vehicle.
* __Spd__: An integer that represents the speed mph(miles per hour) of the vehicle that ranges from 0-100.
* __XWay__: Identifies the highway from which the position report is emitted (0 . . .L−1). //L is the Lane.
* __Lane__: Identifies the lane of the highway from which the position report is emitted and ranges from 0-4.
        * 0 -> Entrance Ramp(ENTRY).
        * 1 to 3 -> Travel Lane(TRAVEL).
        * 4 -> Exit Ramp(EXIT).
* __Dir__: Indicates the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling and ranges from 0-1.
* __Seg__: Identifies the segment from which the position report is emitted and ranges from 0-99.
* __Pos__: Identifies the horizontal position of the vehicle as the number of meters from the westernmost point on the highway and ranges from 0-527999.

## Main goal of project:
Develop a Flink application implementing 3 functionalities:

__Speed Radar__: detect cars that overcome the speed limit of 90 mph.
__Average Speed Control__: detects cars with an average speed higher than 60 mph betweensegments 52 and 56 (both included) in both directions.
__Accident Reporter__: detects stopped vehicles on any segment. A vehicle is stopped when it reports at least 4 consecutive events from the same position.

## Important Notes

1. All metrics must take into account the direction field.
2. A given vehicle could report more than 1 event for the same segment.
3. Event time must be used for timestamping.
4. Cars that do not complete the segment (52-56) are not taken into account by the average speed control. For example 52->54  or 55->56. 
5. A car can be stopped on the same position for more than 4 consecutive events. An accident report must be sent for each group of 4 events. 
For example, the next figure shows 8 events for the car with identifier VID=3:


|Time   |VID   |Spd   |XWay   |Lane   |Dir   |Seg   |Pos   |
|------:|-----:|-----:|------:|------:|-----:|-----:|-----:|
|    870|     3|     0|      0|      1|     0|    26|139158|
|    900|     3|     0|      0|      1|     0|    26|139158|
|    930|     3|     0|      0|      1|     0|    26|139158|
|    960|     3|     0|      0|      1|     0|    26|139158|
|    990|     3|     0|      0|      1|     0|    26|139158|
|   1020|     3|     0|      0|      1|     0|    26|139158|
|   1050|     3|     0|      0|      1|     0|    26|139158|
|   1080|     3|     0|      0|      1|     0|    26|139158|
       
The accident reporter should generate 5 accident alerts. (870->960, 900->990, 930->1020, 960->1050, 990->1080).

## Program INPUT:
* The Java program will read the events from a CSV with the format: Time, VID, Spd, XWay, Lane, Dir, Seg, Pos.

## Program OUPUT:
* The program must generate 3 output CSV files
1. __speedfines.csv__: to store the output of the speed radar. Format: Time, VID, XWay, Seg, Dir, Spd.
2. __avgspeedfines.csv__: to store the output of the average speed control. Format: Time1, Time2, VID, XWay, Dir, AvgSpd, where Time1 is the time of the first event of the segment and Time2 is the time of the last event of the segment.
2. __accidents.csv__: to store the output of the accident detector. Format: Time1, Time2, VID, XWay, Seg, Dir, Pos, where Time1 is the time of the first event the car stops and Time2 is the time of the fourth event the car reports to be stopped.
 
 ## Dataset
 The project dataset is in the **traffic-3xways-new.7z** compressed file.
 
 ## Authors
 Daniel Reis<br>
 Afonso Castro
 
