package com.smallspanner.dmcache;

import java.io.Serializable;
import java.lang.Math;
import java.nio.ByteBuffer;
import org.testng.annotations.*;

class People implements Serializable {
    public String name;
    public int age;
}

public class APITest {
    @Test(dataProvider = "varintset")
    public void uint2varuint(int i, byte [] bs) {
        byte [] xs = API.uint2varuint(i);
        assert cmp(xs, bs);
    }

    @Test(dataProvider = "varintset")
    public void varuint2uint(int i, byte [] bs) {
        ByteBuffer bb = ByteBuffer.allocate(bs.length + 1);
        bb.put(bs);
        bb.rewind();
        int j = API.varuint2uint(bb);
        assert i == j;
    }

    @Test(dataProvider = "intset")
    public void int2bytes(int i, byte [] bs) {
        byte [] xs = API.int2bytes(i);
        assert cmp(xs, bs);
    }

    @Test(dataProvider = "intset")
    public void bytes2int(int i, byte [] bs) {
        int j = API.bytes2int(bs);
        assert i == j;
    }

    @Test(dataProvider = "longset")
    public void long2bytes(long l, byte [] bs) {
        byte [] xs = API.long2bytes(l);
        assert cmp(xs, bs);
    }

    @Test(dataProvider = "longset")
    public void bytes2long(long l, byte [] bs) {
        long k = API.bytes2long(bs);
        assert l == k;
    }

    @Test(dataProvider = "kvset", dependsOnMethods={"uint2varuint"})
    public void put(String key, String value) throws Exception {
        assert API.put("/tmp/kad.ipc", key, value.getBytes("UTF-8"));
    }

    @Test(dataProvider = "kvset", dependsOnMethods={"put", "varuint2uint"})
    public void get(String key, String value) throws Exception {
        byte[] bs = API.get("/tmp/kad.ipc", key);
        assert new String(bs, "UTF-8").equals(value);
    }

    @Test(dataProvider = "intkvset", dependsOnMethods={"uint2varuint", "int2bytes"})
    public void putInt(String key, int value) {
        assert API.putInt(key, value);
    }

    @Test(dataProvider = "intkvset", dependsOnMethods={"putInt", "varuint2uint", "bytes2int"})
    public void getInt(String key, int value) throws Exception {
        int i = API.getInt(key);
        assert i == value;
    }

    @Test(dataProvider = "longkvset", dependsOnMethods={"uint2varuint", "long2bytes"})
    public void putLong(String key, long value) {
        assert API.putLong(key, value);
    }

    @Test(dataProvider = "longkvset", dependsOnMethods={"putLong", "varuint2uint", "bytes2long"})
    public void getLong(String key, long value) throws Exception {
        long l = API.getLong(key);
        assert l == value;
    }

    @Test(dataProvider = "floatkvset", dependsOnMethods={"uint2varuint", "putInt"})
    public void putFloat(String key, float value) {
        assert API.putFloat(key, value);
    }

    @Test(dataProvider = "floatkvset", dependsOnMethods={"putFloat", "varuint2uint", "getInt"})
    public void getFloat(String key, float value) throws Exception {
        float f = API.getFloat(key);
        assert Math.abs(f - value) < 0.000001;
    }

    @Test(dataProvider = "doublekvset", dependsOnMethods={"uint2varuint", "putLong"})
    public void putDouble(String key, double value) {
        assert API.putDouble(key, value);
    }

    @Test(dataProvider = "doublekvset", dependsOnMethods={"putDouble", "varuint2uint", "getLong"})
    public void getDouble(String key, double value) throws Exception {
        double d = API.getDouble(key);
        assert Math.abs(d - value) < 0.0000000001;
    }

    @Test(dataProvider = "stringkvset", dependsOnMethods={"uint2varuint"})
    public void putString(String key, String value) throws Exception {
        assert API.putString(key, value);
    }

    @Test(dataProvider = "stringkvset", dependsOnMethods={"putString", "varuint2uint"})
    public void getString(String key, String value) throws Exception {
        assert API.getString(key).equals(value);
    }

    @Test(dataProvider = "serialkvset", dependsOnMethods={"uint2varuint"})
    public void putSerializable(String key, Serializable value) {
        assert API.putSerializable(key, value);
    }

    @Test(dataProvider = "serialkvset", dependsOnMethods={"putSerializable", "varuint2uint"})
    public void getSerializable(String key, Serializable value) throws Exception {
        People p = (People) API.getSerializable(key);
        People q = (People) value;
        assert p.name.equals(q.name) && p.age == q.age;
    }

    @DataProvider(name = "kvset")
    private Object [][] kvset() {
        return new Object [][] {
            {"A", "a"},
            {"B", "b"},
            {"C", "c"}
        };
    }

    @DataProvider(name = "intkvset")
    private Object [][] intkvset() {
        return new Object [][] {
            {"1", 1},
            {"2", 2},
            {"3", 3}
        };
    }

    @DataProvider(name = "longkvset")
    private Object [][] longkvset() {
        return new Object [][] {
            {"100000000000", 100000000000l},
            {"200000000000", 200000000000l},
            {"300000000000", 300000000000l}
        };
    }

    @DataProvider(name = "floatkvset")
    private Object [][] floatkvset() {
        return new Object [][] {
            {"1.23", 1.23f},
            {"123.456", 123.456f},
            {"12345.6789", 12345.6789f}
        };
    }

    @DataProvider(name = "doublekvset")
    private Object [][] doublekvset() {
        return new Object [][] {
            {"1.0000000023", 1.0000000023d},
            {"1230000.00000456", 1230000.00000456d},
            {"1020304050.60708090", 1020304050.60708090d}
        };
    }

    @DataProvider(name = "stringkvset")
    private Object [][] stringkvset() {
        return new Object [][] {
            {"dm-cache-java-sdk-test1", "Hello World"},
            {"dm-cache-java-sdk-test2", "This is a test."},
            {"dm-cache-java-sdk-test3", "It's hard to do a full test."}
        };
    }

    @DataProvider(name = "serialkvset")
    private Object [][] serialkvset() {
        People anna = new People();
        anna.name = "Anna";
        anna.age = 4;
        People elsa = new People();
        elsa.name = "Elsa";
        elsa.age = 7;
        return new Object [][] {
            {"Anna", anna},
            {"Elsa", elsa}
        };
    }

    @DataProvider(name = "varintset")
    private Object [][] varintset() {
        byte [] bs100 = {0x64};
        byte [] bs200 = {(byte)0xC8, 0x01};
        byte [] bs80000 = {(byte)0x80, (byte)0xF1, 0x04};
        byte [] bs3000000 = {(byte)0xC0, (byte)0x8D, (byte)0xB7, 0x01};
        byte [] bs400000000 = {(byte)0x80, (byte)0x88, (byte)0xDE, (byte)0xBE, 0x01};
        return new Object [][] {
            {100, bs100},
            {200, bs200},
            {80000, bs80000},
            {3000000, bs3000000},
            {400000000, bs400000000}
        };
    }

    @DataProvider(name = "intset")
    private Object [][] intset() {
        byte [] bs256 = {0x00, 0x00, 0x01, 0x00};
        byte [] bs65535 = {0x00, 0x00, (byte)0xFF, (byte)0xFF};
        return new Object [][] {
            {256, bs256},
            {65535, bs65535}
        };
    }

    @DataProvider(name = "longset")
    private Object [][] longset() {
        byte [] bs256 = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00};
        byte [] bs15679000001 = {0x00, 0x00, 0x00, 0x03, (byte)0xa6, (byte)0x8a, (byte)0x8d, (byte)0xc1};
        byte [] bs189641242259741648 = {0x02, (byte)0xa1, (byte)0xbd, (byte)0xb4, (byte)0x7f, (byte)0xa7, (byte)0xdb, (byte)0xd0};
        return new Object [][] {
            {256l, bs256},
            {15679000001l, bs15679000001},
            {189641242259741648l, bs189641242259741648}
        };
    }

    private boolean cmp(byte [] as, byte [] bs) {
        if (as.length == bs.length) {
            for (int i = 0; i < as.length; i ++) {
                if (as[i] != bs[i]) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }
}
