package Index.util;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class MyArrayWritable extends ArrayWritable {
    public MyArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    public MyArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
        super(valueClass, values);
    }

    public MyArrayWritable(String[] strings) {
        super(strings);
    }

    @Override
    public Writable[] get() {
        return super.get();
    }

    @Override
    public String toString() {
        return Arrays.toString(get());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        System.out.println("write method called");
        super.write(out);
    }
}
