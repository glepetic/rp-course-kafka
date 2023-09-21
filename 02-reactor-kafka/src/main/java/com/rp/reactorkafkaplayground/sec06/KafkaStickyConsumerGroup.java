package com.rp.reactorkafkaplayground.sec06;

public class KafkaStickyConsumerGroup {

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
                #3: 2
             */
        }

    }

    private static class Consumer3 {

        public static void main(String[] args) {
            KafkaStickyConsumer.start("3");
            /*
                #3: 1
             */
        }

    }

}
