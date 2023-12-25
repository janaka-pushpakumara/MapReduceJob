/*
 * Copyright (c) 2023, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package week4;

import com.opencsv.CSVReader;

import java.io.FileReader;
import java.io.StringReader;
import java.util.ArrayList;

public class CSVUtils {

/*    public static void main(String[] args) {
        ReadCSVFileAndGetPopularityList();
        ReadCSVFileAndGetPopularityList();
        ReadCSVFileAndGetVoteCountList();
    }*/

    public static ArrayList<Double> ReadCSVFileAndGetPopularityList() {
        CSVReader reader = null;
        ArrayList list = new ArrayList();
        try {
            reader = new CSVReader(new FileReader("/home/janaka/Documents/BDP_lab/MoviesTopRated.csv"));
            String[] nextLine;
            //read one line at a time
            int i = 0;
            while ((nextLine = reader.readNext()) != null) {
                list.add(nextLine[5]);


                if (i != 0 && nextLine[5] != null && !nextLine[5].isEmpty() && !nextLine[5].equals(" ")) {
                    double d = Double.parseDouble(nextLine[5]);
                    if (d > 500) {
                        System.out.println("True");
                    }
                }

                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(list);
        return list;
    }

    public static ArrayList<Double> ReadCSVFileAndGetVoteAVGList() {
        CSVReader reader = null;
        ArrayList list = new ArrayList();
        try {
            reader = new CSVReader(new FileReader("/home/janaka/Documents/BDP_lab/MoviesTopRated.csv"));
            String[] nextLine;
            //read one line at a time
            while ((nextLine = reader.readNext()) != null) {
                list.add(nextLine[7]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(list);
        return list;
    }

    public static ArrayList<Double> ReadCSVFileAndGetVoteCountList() {
        CSVReader reader = null;
        ArrayList list = new ArrayList();
        try {

            reader = new CSVReader(new FileReader("/home/janaka/Documents/BDP_lab/MoviesTopRated.csv"));
            String[] nextLine;
            //read one line at a time
            while ((nextLine = reader.readNext()) != null) {
                list.add(nextLine[8]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(list);
        return list;
    }

    public static ArrayList<String[]> ReadCSVFile(String text) {
        CSVReader reader = null;
        ArrayList list = new ArrayList();
        try {
            //reader = new CSVReader(new FileReader("/home/janaka/Documents/BDP_lab/MoviesTopRated.csv"));
            StringReader rowfile = new StringReader(text);
            reader = new CSVReader(rowfile);
            String[] nextLine;
            //read one line at a time
            while ((nextLine = reader.readNext()) != null) {
                list.add(nextLine);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }


}
