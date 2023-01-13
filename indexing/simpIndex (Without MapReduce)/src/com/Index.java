package com;

import java.io.*;
import java.util.*;

public class Index {

    private static File[] getFiles(String path) {
        File file = new File(path);
        return file.listFiles();
    }

    private static boolean isWord(String str) {
        // function for judging whether string str is a word
        boolean flag = true;
        for(int i = 0; i < str.length(); i++) {
            if((str.charAt(i) > 'z' || str.charAt(i) < 'a') &&
                    (str.charAt(i) > 'Z' || str.charAt(i) < 'A')){
                flag = false;
                break;
            }
        }
        return flag;
    }

    /*
    private static HashMap<String, Integer> wordcount(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        HashMap<String, Integer> count = new HashMap<>();
        String line;
        while((line = reader.readLine()) != null) { // read line
//            System.out.println(line);
            String[] strings = line.split("\\s+|,|\\.|:"); // split line to words
            for (String str: strings) {
                boolean flag = isWord(str);
                if(!flag || str.equals("")) continue;
                if(count.get(str.toLowerCase()) == null) {
                    count.put(str.toLowerCase(), 1); // count++
                }else{
                    count.replace(str.toLowerCase(), count.get(str.toLowerCase())+1);
                }
            }
        }

        reader.close();

        return count;
    }
     */

    private static HashMap<String, ArrayList<String>> index(File[] files) throws IOException {
        HashMap<String, ArrayList<String>>index = new HashMap<>();
        for(File file : files) {
            String filename = file.getName();
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            HashSet<String> visited = new HashSet<>();
            while ((line = reader.readLine()) != null) { // read line
                String[] words = line.split("\\s+|,|\\.|:"); // split line to words
                for (String word: words) {
                    boolean flag = isWord(word);
                    if(!flag || word.equals("") || !visited.add(word)) continue;
                    word = word.toLowerCase();
                    index.computeIfAbsent(word, k -> new ArrayList<>());
                    ArrayList<String>val = index.get(word);
                    val.add(filename);
                    index.put(word, val);
                }
            }
        }

        return index;
    }

    // write index to file
    private static void output(HashMap<String, ArrayList<String>> index, String out_path) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(out_path));
        for (Map.Entry<String, ArrayList<String>> stringIntegerEntry : index.entrySet()) {
            out.write(((Map.Entry<?, ?>) stringIntegerEntry).getKey() + "\t");
            out.write(((Map.Entry<?, ?>) stringIntegerEntry).getValue().toString() + "\n");
        }

        out.close();
    }

    private static String pathPreprocess(String str) {
        // judge whether the last character in the path is '/'
        // if no, add a '/' at the end
        if(str.charAt(str.length() - 1) != '/'){
            str = str + '/';
        }
        return str;
    }

    // for sorting we use TreeMap, it is based on Red-Black Tree
    // Time Complexity: Build with N elements - O(NlogN) (Maintaining the tree with O(logN)), Search - O(logN)
    public static LinkedHashMap<String, ArrayList<String>> sortedHashMap(HashMap<String, ArrayList<String>> hashMap) {
        TreeMap<String, ArrayList<String>> treeMap = new TreeMap<>();

        treeMap.putAll(hashMap); // Pass all the elements of the hashMap to treeMap

        LinkedHashMap<String, ArrayList<String>> linkedHashMap = new LinkedHashMap<>();
        linkedHashMap.putAll(treeMap);
        // use linkedHashMap to maintain the insert sequence

        return linkedHashMap;
    }


    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();

        if(args.length < 2){
            throw new IOException("Usage: Index <inputDirectory> <outputDirectory>");
        }
        String inputPath = pathPreprocess(args[0]);
        String outputPath = pathPreprocess(args[1]);

        File[] files = getFiles(inputPath);


        HashMap<String, ArrayList<String>> index = index(files);
        LinkedHashMap<String, ArrayList<String>> sorted_index = sortedHashMap(index);
        // embedded the sort method for sorting the index
        // although sorted index is useless for our mission
        // the hadoop sort its output by nature
        // to be fair in time cost counting, we have to implement this as well

//        for (Map.Entry<String, ArrayList<String>> stringIntegerEntry : index.entrySet()) {
//            System.out.print("word: " + ((Map.Entry<?, ?>) stringIntegerEntry).getKey() + " in ");
//            System.out.println("" + ((Map.Entry<?, ?>) stringIntegerEntry).getValue());
//        }

        output(sorted_index, outputPath + "out_index.txt");

        long endTime = System.currentTimeMillis();
        double timeDiff = (double) (endTime - startTime) / 1000;
        System.out.println("Time Cost (Seconds): " + timeDiff);
        // the time cost counting in hadoop counts the total time of the job,
        // therefore we do the same to be fair
    }
}
