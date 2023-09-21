package com.rp.reactorkafkaplayground.sec05;

import com.rp.reactorkafkaplayground.sec06.KafkaStickyConsumer;

public class KafkaConsumerGroup {

    /*
    This uses default RangeAssignor

    Takes all partitions and sorts them -> 0, 1, 2
    Take all the members and sort them by id -> 1, 2, 3

    For partitions 0,1,2 amd consumers 1,2 -> 1 gets 0,1 and 2 gets 2
     */

    private static class Consumer1 {

        public static void main(String[] args) {
            KafkaStickyConsumer.start("1");
            /*
                #1: 0, 1, 2
                #2: 0, 1
                #3: 0
             */

        }

    }

    private static class Consumer2 {

        public static void main(String[] args) {
            KafkaStickyConsumer.start("2");
            /*
                #2: 2
                #3: 1
             */
        }

    }

    private static class Consumer3 {

        public static void main(String[] args) {
            KafkaStickyConsumer.start("3");
            /*
                #3: 2
             */
        }

    }

}
