package master2017.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.HashSet;


 /**
 * Vehicle Telematics
 *
  * In this project we consider that each vehicle reports a position event every 30 seconds with the following format: Time, VID, Spd, XWay, Lane, Dir, Seg, Pos.
  *
  *     Time: A timestamp (integer) in seconds identifying the time at which the position event was emitted.
  *     VID: An integer that identifies the vehicle
  *     Spd: An integer that represents the speed mph(miles per hour) of the vehicle that ranges from 0-100
  *     XWay: Identifies the highway from which the position report is emitted (0 . . .L−1). //L is the Lane
  *     Lane: Identifies the lane of the highway from which the position report is emitted and ranges from 0-4
  *         0 -> Entrance Ramp(ENTRY)
  *         1 to 3 -> Travel Lane(TRAVEL)
  *         4 -> Exit Ramp(EXIT)
  *     Dir: Indicates the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling and ranges from 0-1
  *     Seg: Identifies the segment from which the position report is emitted and ranges from 0-99
  *     Pos: Identifies the horizontal position of the vehicle as the number of meters from the westernmost point on the highway and ranges from 0-527999
  *
  * The main goal of this project is to develop a Flink application implementing 3 functionalities:
  *
  *     Speed Radar: detect cars that overcome the speed limit of 90 mph
  *     Average Speed Control: detects cars with an average speed higher than 60 mph betweensegments 52 and 56 (both included) in both directions.
  *     Accident Reporter: detects stopped vehicles on any segment. A vehicle is stopped when it reports at least 4 consecutive events from the same position.
  *
  * Some important notes should be taken in consideration:
  *
  *     1) All metrics must take into account the direction field.
  *     2) A given vehicle could report more than 1 event for the same segment.
  *     3) Event time must be used for timestamping.
  *     4) Cars that do not complete the segment (52-56) are not taken into account by the average speed control. For example 52->54 or 55->56.
  *     A car can be stopped on the same position for more than 4 consecutive events. An accident report must be sent for each group of 4 events.
  *     For example, the next figure shows 8 events for the car with identifier VID=3:
  *      ________________________
  *     |870 ,3,0,0,1,0,26,139158|
  *     |900 ,3,0,0,1,0,26,139158|
  *     |930 ,3,0,0,1,0,26,139158|
  *     |960 ,3,0,0,1,0,26,139158|
  *     |990 ,3,0,0,1,0,26,139158|
  *     |1020,3,0,0,1,0,26,139158|
  *     |1050,3,0,0,1,0,26,139158|
  *     |1080,3,0,0,1,0,26,139158|
  *     ||||||||||||||||||||||||||
  *
  *     The accident reporter should generate 5 accident alerts. (870->960, 900->990, 930->1020, 960->1050, 990->1080).
  *
  * Program INPUT:
  *
  *     - The Java program will read the events from a CSV with the format: Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
  *
  * Program OUPUT:
  *
  *     - The program must generate 3 output CSV files
  *         1) speedfines.csv: to store the output of the speed radar. Format: Time, VID, XWay, Seg, Dir, Spd
  *         2) avgspeedfines.csv: to store the output of the average speed control.
  *             Format: Time1, Time2, VID, XWay, Dir, AvgSpd, where Time1 is the time of the first event of the segment and Time2 is the time of the last event of the segment.
  *         3) accidents.csv: to store the output of the accident detector.
  *             Format: Time1, Time2, VID, XWay, Seg, Dir, Pos, where Time1 is the time of the first event the car stops and Time2 is the time of the fourth event the car reports to be stopped.
  *
  *
 * @author      Daniel Reis
 * @author      Afonso Castro
 */

public class VehicleTelematics {

    public static void main(String[] args) throws Exception {

        //The program arguments with the input Dataset and the Folder with all the output files from the program analysis
        String DATASET_PATH = args[0];
        String OUTPUT_FOLDER = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // We use EventTime because it is the time that each individual event occurred on its producing device.
        // This time is typically embedded within the records before they enter Flink and that event timestamp can be extracted from the record.
        // An hourly event time window will contain all records that carry an event timestamp that falls into that hour, regardless of when the records arrive, and in what order they arrive.
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Define the DataStreamSource for the Dataset
        DataStreamSource<String> vehicleRecordDataStream = env.readTextFile(DATASET_PATH).setParallelism(1);


        //Parse and load all the vehicle records
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleRecords = vehicleRecordDataStream.map(new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String s) throws Exception {
                String[] records = s.split(",");
                if (records.length != 8)
                    return null;
                return new Tuple8<>(
                        Integer.parseInt(records[0]), // Time
                        Integer.parseInt(records[1]), // VID
                        Integer.parseInt(records[2]), // Spd
                        Integer.parseInt(records[3]), // XWay
                        Integer.parseInt(records[4]), // Lane
                        Integer.parseInt(records[5]), // Dir
                        Integer.parseInt(records[6]), // Seg
                        Integer.parseInt(records[7])  // Pos
                );
            }
        }).setParallelism(1);

        //Retrieve and register all the vehicles with a speed above 90mph
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedfines = getSpeedFines(vehicleRecords);
        speedfines.writeAsCsv(OUTPUT_FOLDER + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //Retrive the averate speed of vehicles hith an average speed higher than 60mph between segments 52 and 56(inclusive) taking in consideration the XWay(Highway) and the Dir(Direction)
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> avgSpeedControl = getAVGSpeedFines(vehicleRecords);
        avgSpeedControl.writeAsCsv(OUTPUT_FOLDER + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //Retrieves all the accidents. We consider an accident when a vehicle is stopped when it reports at least 4 consecutive events from the same position
        SingleOutputStreamOperator<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> accidents = getAccidents(vehicleRecords);
        accidents.writeAsCsv(OUTPUT_FOLDER + "/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //Start Program execution
        env.execute("VehicleTelematicsProject");
    }

     /** NOT DONE
      * This function returns all the vehicles that are stopped on any segment. A vehicle is stopped when it reports at least 4 consecutive events from the same position.
      * As we can see in the notes above, we should take in consideration the records VID,XWay and Dir because we can have several different registers for the same vehicle.
      * Theoretically its not possible for a vehicle to have different XWay and Dir on consecutives Timestamps but we considered in the keys to make the program more robust.
      * That's why we want the values ordered and grouped by their VID,XWay and Dir
      * Using a countWindow, we define the window size and the value of the slide to update our window values.
      * After that, we apply our window and collect all the accident values
      *
      * @param  vehicleRecords This parameter is the hole parsed/transformed dataset ready for the analysis.
      * @return The results of the analysis in a Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> that represent the Time1, Time2, VID, XWay, Seg, Dir, Pos.
      * */
    private static SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> getAccidents(SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleRecords) {
        return vehicleRecords
                .keyBy(1,3,5)
                .countWindow(4,1)
                .apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow>() {
                    @Override
                    public void apply(Tuple tuple, GlobalWindow window, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
                        //Our window records should be 4 or more.
                        if(Iterables.size(input)<4)
                            return;

                        boolean moved = false;
                        Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> first = input.iterator().next();
                        Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> last = first;

                        for(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> record : input){
                            if(record.equals(first))
                                continue;
                            if(!record.f7.equals(first.f7)){ //Pos
                                moved=true;
                                break;
                            }
                            last=record;
                        }

                        if(!moved){
                            Tuple7 accident = new Tuple7(first.f0,last.f0,first.f1,first.f3,first.f6,first.f5,first.f7);
                            out.collect(accident);
                        }

                    }
                });
    }

     /**
      * This function returns all the vehicles that have an average speed superior to 60mph between the segments 52 and 56 (this bound is inclusive).
      * The approach to this problem was applying a filter in order to get all the records between the 2 inclusive bounds.
      * After that, we assign watermarks taking in consideration the Timestamp of our data. Since the watermarks use milliseconds and our data Timestamp is in seconds, we need to multiply it by 1000.
      * As we can see in the notes above, we should take in consideration the records VID,XWay and Dir because we can have several different registers for the same vehicle.
      * Theoretically its not possible for a vehicle to have different XWay and Dir on consecutives Timestamps but we considered in the keys to make the program more robust.
      * That's why we want the values ordered and grouped by their VID,XWay and Dir
      * We apply a window gap of 31 seconds because the records Timestamp starts on 0 and go to 30.
      * Then we apply the window
      *
      * @param  vehicleRecords This parameter is the hole parsed/transformed dataset ready for the analysis.
     * @return The results of the analysis in a Tuple6<Integer, Integer, Integer, Integer, Integer, Double> that represent the Time1, Time2, VID, XWay, Dir, AvgSpd.
     * */
    private static SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> getAVGSpeedFines(SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleRecords) {
        return vehicleRecords
                .filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple8) throws Exception {
                        return tuple8.f6 <= 56 && tuple8.f6 >= 52;
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                        return element.f0 * 1000;
                    }
                })
                .keyBy(1, 3, 5) // VID,XWay and Dir
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out) throws Exception {
                        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> s52 = null, s56 = null; //Register the beginning and end of the segments.
                        Double avgSpeed = 0D; //The variable that accumulates the average of the segments speed
                        int totalRecords = Iterables.size(input); // Total records that exist in the current window
                        HashSet<Integer> distinctSeg = new HashSet<>(); // Here we register all the distinct segments where the vehicle goes through.

                        //Wee need to have more that 4 records for each window
                        if (totalRecords < 4)
                            return;


                        for (Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> record : input) {
                            distinctSeg.add(record.f6); //Register the current segment and add it to the hashset if it doesnt exist.

                            // In this if, we define whats the s52 record and the s56 record.
                            // As said before in the project specification, we should take in consideration both direction
                            // p.e: a vehicle can start the segment on 52 and to go 56 or start in 56 and go to 52.
                            if (record.f6.equals(52)) {
                                if (s52 != null) {
                                    if(record.f5.equals(0)){
                                        if (record.f0 < s52.f0)
                                            s52 = record;
                                    }else if(record.f5.equals(1))
                                        if(record.f0 > s52.f0)
                                            s52 = record;
                                }else
                                    s52 = record;
                            }else if (record.f6.equals(56)) {
                                if(s56 != null){
                                    if(record.f5.equals(0)){
                                        if(record.f0 > s56.f0)
                                            s56=record;
                                    }else if(record.f5.equals(1)){
                                        if(record.f0 < s56.f0)
                                            s56=record;
                                    }
                                }else{
                                    s56=record;
                                }
                            }
                            avgSpeed += record.f2;
                        }
                        avgSpeed /= totalRecords;

                        if (distinctSeg.size() == 5 && avgSpeed >= 60 && s52 != null && s56 != null) {
                            int time1 = s52.f0 < s56.f0 ? s52.f0 : s56.f0;
                            int time2 = s52.f0 < s56.f0 ? s56.f0 : s52.f0;

                            out.collect(new Tuple6<>(time1, time2, s52.f1, s52.f3, s52.f5, avgSpeed));
                        }
                    }
                });
    }

    /**
     * This function returns all the vehicles that have a speed superior to 90mph from the input records.
     * The approach to this problem was applying a filter in order to get all the records where the speed is above the 90mph.
     * After the filter, we make a simple map to transform the raw data previously obtained from the filter to the requested format.
     *
     * @param  vehicleRecords This parameter is the hole parsed/transformed dataset ready for the analysis.
     * @return The results of the analysis in a Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> that represent Time, VID, XWay, Seg, Dir, Spd.
     * */
    private static SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> getSpeedFines(SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleRecords) {
       return vehicleRecords.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
           @Override
           public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple8) throws Exception {
               return tuple8.f2 > 90;
           }
       }).map(new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
           @Override
           public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple8) throws Exception {
               return new Tuple6<>(tuple8.f0,tuple8.f1,tuple8.f3,tuple8.f6,tuple8.f5,tuple8.f2);
           }
       });
    }

}
