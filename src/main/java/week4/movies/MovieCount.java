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

package week4.movies;

import java.util.ArrayList;

import static week4.CSVUtils.ReadCSVFile;

public class MovieCount {









    public static void main(String[] args) {
        m();
    }

    static void m() {

        ArrayList<String[]> movies = ReadCSVFile("");
        int i = 0;
        int popularityCount = 0;
        int voteAVGCount = 0;
        int voteCount = 0;

        try {
            for (String[] movie : movies) {

                String popularity = movie[5];
                if (i != 0 && popularity != null && !popularity.isEmpty() && !popularity.equals(" ")) {
                    double d = Double.parseDouble(popularity);
                    if (d > 500) {
                        popularityCount++;
                        System.out.println("popularity> 500 : " + popularityCount);
                    }
                }

                String vote_avg = movie[7];
                if (i != 0 && vote_avg != null && !vote_avg.isEmpty() && !vote_avg.equals(" ")) {
                    double d = Double.parseDouble(vote_avg);
                    if (d > 8.0) {
                        voteAVGCount++;
                        System.out.println("vote_avg > 8.0 : " + voteAVGCount);
                    }
                }

                String vote_count = movie[8];
                if (i != 0 && vote_count != null && !vote_count.isEmpty() && !vote_count.equals(" ")) {
                    double d = Double.parseDouble(vote_count);
                    if (d > 10000) {
                        voteCount++;
                        System.out.println("voteCount > 10000 : " + vote_count);
                    }
                }

//                String date = movie[6];
//                String[] dataarry = date.split("-");
//                if (dataarry.length == 3) {
//                    String year = dataarry[0];
//                    System.out.println(year);
//                }

                i++;
            }
            System.out.println("data set size=" + i);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
