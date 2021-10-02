package IMDB_Jobs;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import java.lang.reflect.Array;
import java.util.List;

public class Q1_Job {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        CsvInputFormat<>
        DataSource<String> text = env.readTextFile("C:\\Users\\Robin\\Documents\\Git\\cloudies_assignment5\\Flink Cloud Computing\\Source\\test.txt");
        DataSource<Tuple8<String, Integer, String, String, String, String, String, Boolean>> csvInput = env.readCsvFile("C:\\Users\\Robin\\Documents\\experimentdata\\IMDB\\name.basics.tsv").types(String.class, Integer.class, String.class, String.class, String.class, String.class, String.class, Boolean.class);
//        DataSet<Tuple8<String,Integer, String, String, String, Array, Array, Boolean>> csvInput = env.readCsvFile("C:\\Users\\Robin\\Documents\\experimentdata\\IMDB");
//                .types(String.class, Integer.class, String.class, String.class, String.class, Array.class, Array.class, Boolean.class);
//        FilterOperator<Tuple8<String, Integer, String, String, String, String, String, Boolean>> filtered = csvInput.filter(item -> item.f1 == 0);
        text.collect();

    }
}
