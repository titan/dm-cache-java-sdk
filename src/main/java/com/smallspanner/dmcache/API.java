package com.smallspanner.dmcache;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.nanomsg.NanoLibrary;

public class API {
    static private NanoLibrary nano = null;
    static private String DEFAULT_IPC = "/tmp/kad.ipc";

    static {
        nano = new NanoLibrary();
    }

    static public boolean put(String key, byte [] value) {
        return put(DEFAULT_IPC, key, value);
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
            byte [] kl = uint2varuint(kbs.length);
            byte [] vl = uint2varuint(value.length);
            int cmdlen = 1 + kl.length + kbs.length + vl.length + value.length;
            ByteBuffer buf = ByteBuffer.allocateDirect(cmdlen);
            buf = buf.put((byte) 0x02).put(kl).put(kbs).put(vl).put(value);
            if (cmdlen == nano.nn_send(socket, buf, 0, cmdlen, 0)) {
                buf = ByteBuffer.allocateDirect(32);
                int buflen = nano.nn_recv(socket, buf, 0, 32, 0);
                if (buflen > 0) {
                    if ((buf.get(0) & 0xFF) == 0x82) {
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

    static public boolean putInt(String ipc, String key, int value) {
        byte [] bs = int2bytes(value);
        return put(ipc, key, bs);
    }

    static public boolean putInt(String key, int value) {
        return putInt(DEFAULT_IPC, key, value);
    }

    static public boolean putLong(String ipc, String key, long value) {
        byte [] bs = long2bytes(value);
        return put(ipc, key, bs);
    }

    static public boolean putLong(String key, long value) {
        return putLong(DEFAULT_IPC, key, value);
    }

    static public boolean putFloat(String ipc, String key, float value) {
        byte [] bs = int2bytes(Float.floatToIntBits(value));
        return put(ipc, key, bs);
    }

    static public boolean putFloat(String key, float value) {
        return putFloat(DEFAULT_IPC, key, value);
    }

    static public boolean putDouble(String ipc, String key, double value) {
        byte [] bs = long2bytes(Double.doubleToLongBits(value));
        return put(ipc, key, bs);
    }

    static public boolean putDouble(String key, double value) {
        return putDouble(DEFAULT_IPC, key, value);
    }

    static public byte [] get(String key)
        throws UnknownCmdException, KeyNotFoundException {
        return get(DEFAULT_IPC, key);
    }

    static public byte [] get(String ipc, String key)
        throws UnknownCmdException, KeyNotFoundException {
        byte [] value = null;
        int socket = nano.nn_socket(nano.AF_SP, nano.NN_REQ);
        if (socket < 0) {
            int errn = nano.nn_errno();
            System.out.printf("*** Error: %d - %s\n", errn, nano.nn_strerror(errn));
            return null;
        }
        String addr = "ipc://" + ipc;
        int ep = nano.nn_connect(socket, addr);
        if (ep < 0) {
            int errn = nano.nn_errno();
            System.out.printf("*** Error: %d - %s\n", errn, nano.nn_strerror(errn));
            return null;
        }
        try {
            byte [] kbs = key.getBytes("UTF-8");
            byte [] kl = uint2varuint(kbs.length);
            int cmdlen = 1 + kl.length + kbs.length;
            ByteBuffer buf = ByteBuffer.allocateDirect(cmdlen);
            buf = buf.put((byte) 0x01).put(kl).put(kbs);
            if (cmdlen == nano.nn_send(socket, buf, 0, cmdlen, 0)) {
                buf = ByteBuffer.allocateDirect(4096);
                int buflen = nano.nn_recv(socket, buf, 0, 4096, 0);
                if (buflen > 0) {
                    byte hdr = buf.get();
                    if ((hdr & 0xFF) == 0x81) {
                        int klen = varuint2uint(buf);
                        buf = (ByteBuffer) buf.position(buf.position() + klen);
                        int vlen = varuint2uint(buf);
                        value = new byte [vlen];
                        buf.get(value);
                    } else if (hdr == 0x00) {
                        int elen = varuint2uint(buf);
                        byte [] err = new byte[elen];
                        buf.get(err);
                        throw new KeyNotFoundException(new String(err, "UTF-8"));
                    } else {
                        throw new UnknownCmdException(String.format("Unknown cmd %02x", hdr));
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
        } catch (KeyNotFoundException e) {
            throw e;
        } catch (UnknownCmdException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            nano.nn_shutdown(socket, ep);
            nano.nn_close(socket);
        }
        return value;
    }

    static public int getInt(String ipc, String key)
        throws UnknownCmdException, KeyNotFoundException {
        byte [] bs = get(ipc, key);
        return bytes2int(bs);
    }

    static public int getInt(String key)
        throws UnknownCmdException, KeyNotFoundException {
        return getInt(DEFAULT_IPC, key);
    }

    static public long getLong(String ipc, String key)
        throws UnknownCmdException, KeyNotFoundException {
        byte [] bs = get(ipc, key);
        return bytes2long(bs);
    }

    static public long getLong(String key)
        throws UnknownCmdException, KeyNotFoundException {
        return getLong(DEFAULT_IPC, key);
    }

    static public float getFloat(String ipc, String key)
        throws UnknownCmdException, KeyNotFoundException {
        return Float.intBitsToFloat(getInt(ipc, key));
    }

    static public float getFloat(String key)
        throws UnknownCmdException, KeyNotFoundException {
        return getFloat(DEFAULT_IPC, key);
    }

    static public double getDouble(String ipc, String key)
        throws UnknownCmdException, KeyNotFoundException {
        return Double.longBitsToDouble(getLong(ipc, key));
    }

    static public double getDouble(String key)
        throws UnknownCmdException, KeyNotFoundException {
        return getDouble(DEFAULT_IPC, key);
    }

    static byte [] uint2varuint(int x) {
        x = x & 0xFFFFFFFF;
        byte [] bs = null;
        if (x <= 0x7F) {
            bs = new byte[1];
            bs[0] = (byte) (x & 0x7F);
        } else if (x <= 0x3FFF) {
            bs = new byte[2];
            bs[0] = (byte) (x | 0x80);
            bs[1] = (byte) ((x >> 7) & 0x7F);
        } else if (x <= 0x1FFFFF) {
            bs = new byte[3];
            bs[0] = (byte) (x | 0x80);
            bs[1] = (byte) ((x >> 7) | 0x80);
            bs[2] = (byte) ((x >> 14) & 0x7F);
        } else if (x <= 0xFFFFFFF) {
            bs = new byte[4];
            bs[0] = (byte) (x | 0x80);
            bs[1] = (byte) ((x >> 7) | 0x80);
            bs[2] = (byte) ((x >> 14) | 0x80);
            bs[3] = (byte) ((x >> 21) & 0x7F);
        } else {
            bs = new byte[5];
            bs[0] = (byte) (x | 0x80);
            bs[1] = (byte) ((x >> 7) | 0x80);
            bs[2] = (byte) ((x >> 14) | 0x80);
            bs[3] = (byte) ((x >> 21) | 0x80);
            bs[4] = (byte) ((x >> 28) & 0x0F);
        }
        return bs;
    }

    static int varuint2uint(ByteBuffer bb) {
        int r = 0;
        int s = 0;
        for (int i = 0; i < 5; i ++) {
            int b = (int) bb.get();
            if ((b & 0x80) == 0) {
                r |= ((b & 0x7F) << s);
                return r;
            } else {
                r |= ((b & 0x7F) << s);
                s += 7;
            }
        }
        return r;
    }

    static byte [] int2bytes(int i) {
        byte [] bs = new byte[4];
        bs[0] = (byte)((i >> 24) & 0xFF);
        bs[1] = (byte)((i >> 16) & 0xFF);
        bs[2] = (byte)((i >>  8) & 0xFF);
        bs[3] = (byte)(i & 0xFF);
        return bs;
    }

    static int bytes2int(byte [] bs) {
        int r = 0;
        int s = 24;
        for (int i = 0; i < 4; i ++) {
            r += (((int)bs[i]) & 0xFF) << s;
            s -= 8;
        }
        return r;
    }

    static byte [] long2bytes(long l) {
        byte [] bs = new byte[8];
        bs[0] = (byte)((l >> 56) & 0xFF);
        bs[1] = (byte)((l >> 48) & 0xFF);
        bs[2] = (byte)((l >> 40) & 0xFF);
        bs[3] = (byte)((l >> 32) & 0xFF);
        bs[4] = (byte)((l >> 24) & 0xFF);
        bs[5] = (byte)((l >> 16) & 0xFF);
        bs[6] = (byte)((l >>  8) & 0xFF);
        bs[7] = (byte)(l & 0xFF);
        return bs;
    }

    static long bytes2long(byte [] bs) {
        long r = 0;
        int s = 56;
        for (int i = 0; i < 8; i ++) {
            r += (((long)bs[i]) & 0xFF) << s;
            s -= 8;
        }
        return r;
    }
}
