package com.smallspanner.dmcache;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.nanomsg.NanoLibrary;

class IntBytePair {
    public int i;
    public byte [] bs;
}

public class API {
    static private NanoLibrary nano = null;

    static {
        nano = new NanoLibrary();
    }

    static public boolean put(String ipc, String key, byte [] value) {
        boolean result = false;
        int socket = nano.nn_socket(nano.AF_SP, nano.NN_REQ);
        if (socket < 0) {
            int errn = nano.nn_errno();
            System.out.printf("*** Error: %d - %s\n", errn, nano.nn_strerror(errn));
            return false;
        }
        String addr = "ipc://" + ipc;
        int ep = nano.nn_connect(socket, addr);
        if (ep < 0) {
            int errn = nano.nn_errno();
            System.out.printf("*** Error: %d - %s\n", errn, nano.nn_strerror(errn));
            return false;
        }
        try {
            byte [] kbs = key.getBytes("utf-8");
            byte [] kl = int2varint(kbs.length);
            byte [] vl = int2varint(value.length);
            int cmdlen = 1 + kl.length + kbs.length + vl.length + value.length;
            ByteBuffer buf = ByteBuffer.allocateDirect(cmdlen);
            buf = buf.put((byte) 0x02).put(kl).put(kbs).put(vl).put(value);
            if (cmdlen == nano.nn_send(socket, buf, 0, cmdlen, 0)) {
                buf = ByteBuffer.allocate(32);
                int buflen = nano.nn_recv(socket, buf, 0, -1, 0);
                if (buflen > 0) {
                    if (buf.get(0) == 0x82) {
                        result = true;
                    } else {
                        System.out.printf("*** Error: Put (%s, ...) failed\n", key);
                    }
                } else {
                    if (buflen == 0) {
                        System.out.printf("*** Error: Received nothing\n");
                    } else {
                        int errn = nano.nn_errno();
                        System.out.printf("*** Error: %d - %s\n", errn, nano.nn_strerror(errn));
                    }
                }
            } else {
                System.out.printf("*** Error: Sent size is incorrect.\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        nano.nn_shutdown(socket, ep);
        nano.nn_close(socket);
        return result;
    }

    static public byte [] get(String ipc, String key) {
        return null;
    }

    static private byte [] int2varint(int x) {
        byte [] bs = null;
        if (x <= 0x7F) {
            bs = new byte[1];
            bs[0] = (byte) (x & 0xFF);
        } else if (x <= 0x3FFF) {
            bs = new byte[2];
            bs[0] = (byte) (x & 0xFF);
            bs[1] = (byte) ((x >> 7) & 0x7F);
        } else if (x <= 0x1FFFFF) {
            bs = new byte[3];
            bs[0] = (byte) (x & 0xFF);
            bs[1] = (byte) ((x >> 7) & 0xFF);
            bs[2] = (byte) ((x >> 14) & 0x7F);
        } else if (x <= 0xFFFFFFF) {
            bs = new byte[4];
            bs[0] = (byte) (x & 0xFF);
            bs[1] = (byte) ((x >> 7) & 0xFF);
            bs[2] = (byte) ((x >> 14) & 0xFF);
            bs[3] = (byte) ((x >> 21) & 0x7F);
        } else {
            bs = new byte[5];
            bs[0] = (byte) (x & 0xFF);
            bs[1] = (byte) ((x >> 7) & 0xFF);
            bs[2] = (byte) ((x >> 14) & 0xFF);
            bs[3] = (byte) ((x >> 21) & 0xFF);
            bs[4] = (byte) ((x >> 28) & 0x7F);
        }
        return bs;
    }

    static private IntBytePair varint2int(byte [] bs) {
        int s = 0;
        IntBytePair p = new IntBytePair();
        p.i = 0;
        for (int i = 0; i < bs.length; i ++ ) {
            int b = (int) bs[i];
            if ((b & 0x80) == 0) {
                p.i |= ((b & 0x7F) << s);
                p.bs = Arrays.copyOfRange(bs, i + 1, bs.length);
                return p;
            } else {
                p.i |= ((b & 0x7F) << s);
                s += 7;
            }
        }
        return p;
    }
}
