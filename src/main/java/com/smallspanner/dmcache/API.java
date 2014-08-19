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
}
