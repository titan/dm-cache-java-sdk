package com.smallspanner.dmcache;

import java.nio.ByteBuffer;
import org.testng.annotations.*;

public class APITest {
    @Test(dataProvider = "intset")
    public void uint2varuint(int i, byte [] bs) {
        byte [] xs = API.uint2varuint(i);
        assert cmp(xs, bs);
    }

    @Test(dataProvider = "intset")
    public void varuint2uint(int i, byte [] bs) {
        ByteBuffer bb = ByteBuffer.allocate(bs.length + 1);
        bb.put(bs);
        bb.rewind();
        int j = API.varuint2uint(bb);
        assert i == j;
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

    @DataProvider(name = "kvset")
    private Object [][] kvset() {
        return new Object [][] {
            {"1", "A"},
            {"2", "B"},
            {"3", "C"}
        };
    }

    @DataProvider(name = "intset")
    private Object [][] intset() {
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
