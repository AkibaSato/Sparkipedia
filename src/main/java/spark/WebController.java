package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import java.io.File;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import scala.collection.mutable.WrappedArray;
import java.util.HashSet;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


@Controller
public class WebController {

    private SparkSession spark = makeSession();
    private Dataset<Row> [] results = makeDb();
    private static Long expirationTime = (long)(24 * 60 * 60 * 1000); // 24 hours as a milliseconds

    private Dataset<Row> [] makeDb(){
        // This is objectively a hack on Java
        Dataset<Row> [] routerTable = new Dataset[676];
        for (int i = 0; i < routerTable.length; i++) {
            StringBuilder sb =  new StringBuilder("part-r-00000");
            String intString = String.valueOf(i);
            int length = intString.length();
            int sbLength = sb.length();
            sb.replace(sbLength-length, sbLength, intString);
            String fileName = sb.toString();
            File j = new File("/Users/brad/Desktop/Brandeis2017/Sparkipedia/src/main/resources/partitions/"+fileName);
            if(!j.exists()){
                addBrackets();
            }
            Dataset<Row> results = spark.read()
                    .json("/Users/brad/Desktop/Brandeis2017/Sparkipedia/src/main/resources/partitions/"+fileName);
            results.createOrReplaceTempView("Table");
            routerTable[i] = results;
        }
        return routerTable;
    }

    @RequestMapping("/")
    public String index(Model model) {
        model.addAttribute("errorMessage", "");
        return "index";
    }

    private String addBrackets(){
        FileInputStream instream = null;
        FileOutputStream outstream = null;

        try{
            File infile =new File("/Users/brad/Desktop/Brandeis2017/Sparkipedia/src/main/resources/raw_input.txt");
            File outfile =new File("/Users/brad/Desktop/Brandeis2017/Sparkipedia/src/main/resources/input.json");
            instream = new FileInputStream(infile);
            outstream = new FileOutputStream(outfile);
            String first = "[";
            byte[] b = first.getBytes();
            outstream.write(b, 0, 1);
            byte[] buffer = new byte[1024];
            int length;
            while ((length = instream.read(buffer)) > 0){
                outstream.write(buffer, 0, length);
            }
            //Closing the input/output file streams
            instream.close();
            String second = "]";
            byte[] c = second.getBytes();
            outstream.write(c, 0, 1);
            outstream.close();
            System.out.println("File copied successfully!!");
        }catch(IOException ioe){
            ioe.printStackTrace();
        }
        return "/Users/brad/Desktop/Brandeis2017/Sparkipedia/src/main/resources/input.json";
    }

    @RequestMapping("/search")
    public String search(Model model, String query) {
        try {
            long start = System.currentTimeMillis();
            ArrayList<Link> arr;
            if (query.contains("and")){
                arr = ands(query.toLowerCase());
            } else {
                arr = ors(query.toLowerCase());
            }
            model.addAttribute("links", arr);
            long end = System.currentTimeMillis();
            NumberFormat formatter = new DecimalFormat("#0.00000");
            String time = String.format("Execution time is " + formatter.format((end - start) / 1000d) + " seconds");
            model.addAttribute("runtime", time);
            model.addAttribute("query", query);
            return "search";
        }catch (Exception e ){
            String errorMsg = "Sorry, That query was rejected, we did not know that phrase.";
            model.addAttribute("errorMessage", errorMsg);
            return "index";
        }
    }

    public SparkSession makeSession() {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .config(new SparkConf())
                .getOrCreate();

        return spark;
    }

    public Dataset<Row>[] ReadTable (String query){
        ArrayList<String> positiveWords = WordFetcher(query);
        Dataset<Row> [] resultList = new Dataset[positiveWords.size()];
        int count = 0;
        for (String word : positiveWords) {
            int firstChar = word.charAt(0) - 'a';
            int secondChar = word.charAt(1) - 'a';
            int index = (firstChar * 26 + secondChar) % 676;
            Dataset<Row> r = this.results[index].where("word ='" + word + "'");
            r.cache();
            resultList[count] = r;
            count = count + 1;
        }
        return resultList;
    }

    public ArrayList<Link> ors(String input){
        ArrayList<Link> links = new ArrayList<>();
        Dataset<Row>[] partitionedTables = ReadTable(input);
        int count = partitionedTables.length;
        WrappedArray[] docIDList =  new WrappedArray[count];
        WrappedArray[] urlList =  new WrappedArray[count];
        WrappedArray[] textList =  new WrappedArray[count];
        WrappedArray[] titleList =  new WrappedArray[count];

        for(int i = 0; i < count; i++){
            docIDList[i] = (WrappedArray)partitionedTables[i].select("docID").head().get(0);
            urlList[i] = (WrappedArray)partitionedTables[i].select("url").head().get(0);
            textList[i] = (WrappedArray)partitionedTables[i].select("text").head().get(0);
            titleList[i] = (WrappedArray)partitionedTables[i].select("title").head().get(0);
        }

        int i = 0;
        int[] j = new int[count];
        boolean[] exhausted = new boolean[count];
        int exhaustedCount = 0;

        while (links.size() < 100 && exhaustedCount < count) {
            if (exhausted[i]){
                i++;
                if (i == count)
                    i = 0;
                continue;
            }

            int k = j[i];
            System.out.println("CHECK i: " + i + " k: " + k);
            String DocID = docIDList[i].apply(k).toString();
            String url = docIDList[i].apply(k).toString();
            String text = textList[i].apply(k).toString();
            String title = titleList[i].apply(k).toString();
            Link tmp = new Link(DocID, url, text, title);
            links.add(tmp);

            if (k == docIDList[i].length()-1){
                exhausted[i] = true;
                exhaustedCount ++;
            }else{
                j[i] = j[i] + 1;
            }
            i++;
            if (i == count)
                i = 0;
        }
        return links;
    }

    public ArrayList<Link> ands(String input){
        ArrayList<Link> links = new ArrayList<>();
        Dataset<Row>[] partitionedTables = ReadTable(input);
        int count = partitionedTables.length;
        long start = System.currentTimeMillis();
        int size = 0;
        Object[] sets = new Object[count];
        long one = System.currentTimeMillis();
        NumberFormat formatter = new DecimalFormat("#0.00000");
        System.out.print("GOT RESULT FROM DATABASE " + formatter.format((one - start) / 1000d) + " seconds");
        WrappedArray[] docIDList =  new WrappedArray[count];
        WrappedArray[] urlList =  new WrappedArray[count];
        WrappedArray[] textList =  new WrappedArray[count];
        WrappedArray[] titleList =  new WrappedArray[count];

        for(int i = 0; i < count; i++){
            docIDList[i] = (WrappedArray)partitionedTables[i].select("docID").head().get(0);
            urlList[i] = (WrappedArray)partitionedTables[i].select("url").head().get(0);
            textList[i] = (WrappedArray)partitionedTables[i].select("text").head().get(0);
            titleList[i] = (WrappedArray)partitionedTables[i].select("title").head().get(0);

            HashSet<String> set = new HashSet<>();
            for(int j = 0; j< docIDList[i].length(); j++){
                set.add(docIDList[i].apply(j).toString());
            }
            sets[i] = set;
        }

        int i = 0;
        int[] j = new int[count];
        boolean[] exhausted = new boolean[count];
        int exhaustedCount = 0;
        while (size < 100 && exhaustedCount < count) {
            if (exhausted[i]){
                i++;
                if (i == count)
                    i = 0;
                continue;
            }
            int k = j[i];
            String DocID = docIDList[i].apply(k).toString();
            boolean valid = true;
            for(int w = 0; w < count; w++){
                HashSet<String> tmp = (HashSet<String>)sets[w];
                if(!tmp.contains(DocID)){
                    valid = false;
                    break;
                }
            }

            if (k == docIDList[i].length()-1){
                exhausted[i] = true;
                exhaustedCount ++;
            }else{
                j[i] = j[i] + 1;
            }

            if (!valid){
                i++;
                if (i == count)
                    i = 0;
                continue;
            }

            String url = docIDList[i].apply(k).toString();
            String text = textList[i].apply(k).toString();
            String title = titleList[i].apply(k).toString();

            Link tmp = new Link(DocID, url, text, title);
            links.add(tmp);
            size++;

            i++;
            if (i == count)
                i = 0;
        }
        long end = System.currentTimeMillis();
        System.out.print("Execution time is " + formatter.format((end - start) / 1000d) + " seconds");

        return links;
    }

    public static ArrayList<String> QueryParser(String query){
        query = query.toLowerCase();
        ArrayList<String> wordList = new ArrayList<String>();
        String [] querySplit = query.split(" ");

        for (String word : querySplit) {
            if (!word.equals("and") && !word.equals("or") && !word.equals("not")) {
                if (word.substring(0,3).equals("not")) {
                    word = word.substring(3);
                }
                if (word.substring(0,1).equals("(") || word.substring(0,1).equals("not")) {
                    word = word.substring(1);
                }
                if (word.substring(word.length()-1).equals(")")){
                    word = word.substring(0,word.length()-1);
                }
                wordList.add(word);
            }

        }

        return wordList;

    }


    public static ArrayList<String> GetNegatives (String query) {
        ArrayList<String> negativeList = new ArrayList<String>();
        query = query.toLowerCase();
        if (!query.contains("not")) {
            return negativeList;
        }
        String [] querySplit = query.split(" ");
        boolean seenNot = false;
        for (String word : querySplit) {
            if (word.equals("not")) {
                seenNot = true;
            } else if(seenNot) {
                negativeList.add(word.trim());
                seenNot = false;
            }
        }
        return negativeList;
    }

    public ArrayList<String> getQueryWords(String query){
        ArrayList<String> arr = new ArrayList<String>();
        arr.add("elephant");
        arr.add("zebra");
        return arr;
    }

    public Dataset<Row> getPartitionedPositives  (String query) {
        // FILL THIS IN
        ArrayList<String> words = getQueryWords(query);

        Dataset<Row> allRows = null;
        for(String word: words){
            //
            String l = GetPositives(word);
            // REFACTOR SO THAT YOU USE THE CORRECT spark INSTANCE
            // have a method to figure out the spark instance
            Dataset<Row> tmp = spark.sql(l);
            // add tmp to allRows
            tmp.cache();
        }


        return allRows;
    }

    public String GetPositives (String query) {
        String command = "select * from Table where";
        StringBuilder commandBuilder = new StringBuilder(command);
        String [] querySplit = query.split(" ");
        boolean seenNot = false;
        int countAdded = 0;
        for (String word : querySplit) {
            if (word.equals("not")) {
                seenNot = true;
            } else if((word.equals("and") || word.equals("or")) && countAdded > 0) {
                commandBuilder.append(" or");
            } else if(!seenNot && !word.equals("and") && !word.equals("or") && !word.equals("not")) {
                commandBuilder.append(" word = '" + word.trim() +"'");
                countAdded = countAdded + 1;
            } else if(seenNot) {
                seenNot = false;
            }
        }
        return commandBuilder.toString();
    }

    public static ArrayList<String> WordFetcher(String query){
        query = query.toLowerCase();
        ArrayList<String> wordList = new ArrayList<String>();
        String [] querySplit = query.split(" ");

        for (String word : querySplit) {
            if (!word.equals("and") && !word.equals("or") && !word.equals("not")) {
                if (word.substring(0,3).equals("not")) {
                    word = word.substring(3);
                }
                if (word.substring(0,1).equals("(") || word.substring(0,1).equals("not")) {
                    word = word.substring(1);
                }
                if (word.substring(word.length()-1).equals(")")){
                    word = word.substring(0,word.length()-1);
                }
                wordList.add(word);
            }

        }

        return wordList;

    }

}