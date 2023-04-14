package com.iotek.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderDetail implements Writable {
    private int goodsId;
    private String name = "";
    private int price;//商品价格
    private String unit="";//商品单位
    private int orderId;//订单id
    private int totalPrice;//商品总价

    private int count;//商品数量
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(goodsId);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(price);
        dataOutput.writeUTF(unit);

        dataOutput.writeInt(orderId);
        dataOutput.writeInt(totalPrice);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        goodsId = dataInput.readInt();
        name = dataInput.readUTF();
        price = dataInput.readInt();
        unit = dataInput.readUTF();

        orderId = dataInput.readInt();
        totalPrice = dataInput.readInt();
        count = dataInput.readInt();
    }

    public int getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(int goodsId) {
        this.goodsId = goodsId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public int getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(int totalPrice) {
        this.totalPrice = totalPrice;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "OrderDetail{" +
                "goodsId=" + goodsId +
                ", name='" + name + '\'' +
                ", price=" + price +
                ", unit='" + unit + '\'' +
                ", orderId=" + orderId +
                ", totalPrice=" + totalPrice +
                ", count=" + count +
                '}';
    }
}