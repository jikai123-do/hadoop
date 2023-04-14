package com.iotek.flow.sort;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2018/7/6.
 */
public class FlowBean implements Writable {
    private String phone;
    private long upFlow;
    private long downFlow;
    private long countFlow;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(countFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        phone = dataInput.readUTF();
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        countFlow = dataInput.readLong();
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getCountFlow() {
        return countFlow;
    }

    public void setCountFlow(long countFlow) {
        this.countFlow = countFlow;
    }

    @Override
    public String toString() {
        return  phone + ","+
                upFlow + ","+
                downFlow + ","+
                countFlow;
    }
}
