package com.smallspanner.dmcache;

import java.nio.ByteBuffer;
import org.testng.annotations.*;

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
