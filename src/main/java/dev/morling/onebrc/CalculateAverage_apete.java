/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.ojalgo.concurrent.ProcessingService;
import org.ojalgo.netio.SegmentedFile;
import org.ojalgo.netio.SegmentedFile.Segment;
import org.ojalgo.netio.TextLineReader;
import org.ojalgo.type.CalendarDateUnit;
import org.ojalgo.type.Stopwatch;
import org.ojalgo.type.function.TwoStepMapper;

public class CalculateAverage_apete {

    static final class MeasurementAggregator {

        static double round(final double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        int count = 0;
        double max = Double.NEGATIVE_INFINITY;
        double min = Double.POSITIVE_INFINITY;
        double sum = 0.0;

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(16);
            this.appendTo(builder);
            return builder.toString();
        }

        void appendTo(final StringBuilder builder) {
            builder.append(min).append('/').append(MeasurementAggregator.round(sum / count)).append('/').append(max);
        }

        void merge(final MeasurementAggregator other) {
            if (other.min < min) {
                min = other.min;
            }
            if (other.max > max) {
                max = other.max;
            }
            sum += other.sum;
            count += other.count;
        }

        void put(final double value) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sum += value;
            count++;
        }

    }

    static final class SegmentProcessor implements TwoStepMapper.Mergeable<SegmentedFile.Segment, SortedMap<String, MeasurementAggregator>>,
            TwoStepMapper.Combineable<SegmentedFile.Segment, SortedMap<String, MeasurementAggregator>, SegmentProcessor> {

        static double parseDouble(final String sequence, final int first, final int limit) {

            int unscaled = 0;
            boolean negative = false;

            for (int i = first; i < limit; i++) {

                char digit = sequence.charAt(i);

                switch (digit) {
                case '-':
                    negative = true;
                    break;
                case '.':
                    break;
                default:
                    unscaled = 10 * unscaled + digit - '0';
                    break;
                }
            }

            if (negative) {
                return -unscaled / 10D;
            } else {
                return unscaled / 10D;
            }
        }

        private final Map<String, MeasurementAggregator> myAggregates = new HashMap<>(512);
        private final SegmentedFile mySegmentedFile;

        SegmentProcessor(final SegmentedFile segmentedFile) {
            super();
            mySegmentedFile = segmentedFile;
        }

        @Override
        public void combine(final SegmentProcessor other) {
            this.doMerge(other.getAggregates());
        }

        @Override
        public void consume(final Segment segment) {

            try (TextLineReader reader = mySegmentedFile.newTextLineReader(segment)) {

                reader.processAll(this::doOne);

            } catch (IOException cause) {
                throw new UncheckedIOException(cause);
            }
        }

        @Override
        public SortedMap<String, MeasurementAggregator> getResults() {
            return new TreeMap<>(myAggregates);
        }

        @Override
        public void merge(final SortedMap<String, MeasurementAggregator> other) {
            this.doMerge(other);
        }

        @Override
        public void reset() {
            myAggregates.clear();
        }

        private void doMerge(final Map<String, MeasurementAggregator> other) {
            for (Map.Entry<String, MeasurementAggregator> entry : other.entrySet()) {
                myAggregates.computeIfAbsent(entry.getKey(), k -> new MeasurementAggregator()).merge(entry.getValue());
            }
        }

        private void doOne(final String line) {

            int split = line.indexOf(";");

            String station = line.substring(0, split);
            double temperature = SegmentProcessor.parseDouble(line, split + 1, line.length());

            myAggregates.computeIfAbsent(station, k -> new MeasurementAggregator()).put(temperature);
        }

        Map<String, MeasurementAggregator> getAggregates() {
            return myAggregates;
        }

    }

    private static final File FILE = new File("/Users/apete/Developer/data/1BRC/measurements.txt");

    public static void main(final String[] args) throws IOException {

        Stopwatch stopwatch = new Stopwatch();

        try (SegmentedFile segmentedFile = SegmentedFile.of(FILE)) {

            Map<String, MeasurementAggregator> results = ProcessingService.INSTANCE.reduceCombineable(segmentedFile.segments(),
                    () -> new SegmentProcessor(segmentedFile));

            System.out.println(results);
        }

        System.out.println("Done: " + stopwatch.stop(CalendarDateUnit.SECOND));
    }

}
