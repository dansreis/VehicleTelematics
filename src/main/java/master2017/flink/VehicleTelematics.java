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


public class VehicleTelematics {

    public static void main(String[] args) throws Exception {

        //The program arguments with the input Dataset and the Folder with all the output files from the program analysis
        String DATASET_PATH = args[0];
        String OUTPUT_FOLDER = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Use the Measurement Timestamp of the Event
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
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> avgSpeedControl = getAverageSpeedControl(vehicleRecords);
        avgSpeedControl.writeAsCsv(OUTPUT_FOLDER + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //Retrieves all the accidents. We consider an accident when a vehicle is stopped when it reports at least 4 consecutive events from the same position
        SingleOutputStreamOperator<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> accidents = getAccidents(vehicleRecords);
        accidents.writeAsCsv(OUTPUT_FOLDER + "/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("VehicleTelematicsProject");
    }

    /**
     * DONE
     * */
    private static SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> getAccidents(SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleRecords) {
        return vehicleRecords
                .keyBy(1,3,5)
                .countWindow(4,1)
                .apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow>() {
                    @Override
                    public void apply(Tuple tuple, GlobalWindow window, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {

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

    /** NOT DONE
     * Returns an Image object that can then be painted on the screen.
     * The url argument must specify an absolute. The name
     * argument is a specifier that is relative to the url argument.
     * <p>
     * This method always returns immediately, whether or not the
     * image exists. When this applet attempts to draw the image on
     * the screen, the data will be loaded. The graphics primitives
     * that draw the image will incrementally paint on the screen.
     *
     * @param  vehicleRecords This parameter is the hole parsed/transformed dataset ready for the analysis
     * @return The results of the analysis in a Tuple6<Integer, Integer, Integer, Integer, Integer, Double> that represent the
     * */
    private static SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> getAverageSpeedControl(SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleRecords) {
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
                .keyBy(1, 3, 5) // VID,XWay and DIR
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out) throws Exception {
                        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> s52 = null, s56 = null;
                        Double avgSpeed = 0D;
                        int totalRecords = Iterables.size(input);
                        HashSet<Integer> distinctSeg = new HashSet<>();

                        if (totalRecords < 4)
                            return;


                        for (Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> record : input) {
                            distinctSeg.add(record.f6);

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
     * */
    private static SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> getSpeedFines(SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleRecords) {
       return vehicleRecords.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
           @Override
           public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple8) throws Exception {
               return tuple8.f2>90 ? true:false;
           }
       }).map(new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
           @Override
           public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple8) throws Exception {
               return new Tuple6<>(tuple8.f0,tuple8.f1,tuple8.f3,tuple8.f6,tuple8.f5,tuple8.f2);
           }
       });
    }

}
