package thirdJobTenMostFreqSold;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReverseComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public ReverseComparator() {
            super(Text.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return (-1)* TEXT_COMPARATOR
			        .compare(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof Text && b instanceof Text) {
                return (-1)*(((Text) a)
                        .compareTo((Text) b));
            }
            return super.compare(a, b);
        }
    }