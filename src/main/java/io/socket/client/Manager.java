package io.socket.client;

import io.socket.backo.Backoff;
import io.socket.emitter.Emitter;
import io.socket.parser.DecodingException;
import io.socket.parser.IOParser;
import io.socket.parser.Packet;
import io.socket.parser.Parser;
import io.socket.thread.EventThread;
import okhttp3.Call;
import okhttp3.WebSocket;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manager class represents a connection to a given Socket.IO server.
 */
public class Manager extends Emitter {

    private static final Logger logger = Logger.getLogger(Manager.class.getName());

    /*package*/ enum ReadyState {
        CLOSED, OPENING, OPEN
    }

    /**
     * Called on a successful connection.
     */
    public static final String EVENT_OPEN = "open";

    /**
     * Called on a disconnection.
     */
    public static final String EVENT_CLOSE = "close";

    public static final String EVENT_PACKET = "packet";
    public static final String EVENT_ERROR = "error";

    /**
     * Called on a successful reconnection.
     */
    public static final String EVENT_RECONNECT = "reconnect";

    /**
     * Called on a reconnection attempt error.
     */
    public static final String EVENT_RECONNECT_ERROR = "reconnect_error";

    public static final String EVENT_RECONNECT_FAILED = "reconnect_failed";

    public static final String EVENT_RECONNECT_ATTEMPT = "reconnect_attempt";

    /**
     * Called when a new transport is created. (experimental)
     */
    public static final String EVENT_TRANSPORT = Engine.EVENT_TRANSPORT;

    /*package*/ static WebSocket.Factory defaultWebSocketFactory;
    /*package*/ static Call.Factory defaultCallFactory;

    /*package*/ ReadyState readyState;

    private boolean _reconnection;
    private boolean skipReconnect;
    private boolean reconnecting;
    private boolean encoding;
    private int _reconnectionAttempts;
    private long _reconnectionDelay;
    private long _reconnectionDelayMax;
    private double _randomizationFactor;
    private long _timeout;

    private final Backoff backoff;
    private final URI uri;
    private final List<Packet> packetBuffer;
    private final Queue<On.Handle> subs;
    private final Options opts;
    private final Parser.Encoder encoder;
    private final Parser.Decoder decoder;

    /*package*/ io.socket.engineio.client.Socket engine;

    /**
     * This HashMap can be accessed from outside of EventThread.
     */
    /*package*/ ConcurrentHashMap<String, Socket> nsps;


    public Manager() {
        this(null, null);
    }

    public Manager(URI uri) {
        this(uri, null);
    }

    public Manager(Options opts) {
        this(null, opts);
    }

    public Manager(URI uri, Options opts) {
        if (opts == null) {
            opts = new Options();
        }
        if (opts.path == null) {
            opts.path = "/socket.io";
        }
        if (opts.webSocketFactory == null) {
            opts.webSocketFactory = defaultWebSocketFactory;
        }
        if (opts.callFactory == null) {
            opts.callFactory = defaultCallFactory;
        }
        this.opts = opts;
        nsps = new ConcurrentHashMap<>();
        subs = new LinkedList<>();
        reconnection(opts.reconnection);
        reconnectionAttempts(opts.reconnectionAttempts != 0 ? opts.reconnectionAttempts : Integer.MAX_VALUE);
        reconnectionDelay(opts.reconnectionDelay != 0 ? opts.reconnectionDelay : 1000);
        reconnectionDelayMax(opts.reconnectionDelayMax != 0 ? opts.reconnectionDelayMax : 5000);
        randomizationFactor(opts.randomizationFactor != 0.0 ? opts.randomizationFactor : 0.5);
        backoff = new Backoff()
                .setMin(reconnectionDelay())
                .setMax(reconnectionDelayMax())
                .setJitter(randomizationFactor());
        timeout(opts.timeout);
        readyState = ReadyState.CLOSED;
        this.uri = uri;
        encoding = false;
        packetBuffer = new ArrayList<>();
        encoder = opts.encoder != null ? opts.encoder : new IOParser.Encoder();
        decoder = opts.decoder != null ? opts.decoder : new IOParser.Decoder();
    }

    public boolean reconnection() {
        return _reconnection;
    }

    public Manager reconnection(boolean v) {
        _reconnection = v;
        return this;
    }

    public boolean isReconnecting() {
        return reconnecting;
    }

    public int reconnectionAttempts() {
        return _reconnectionAttempts;
    }

    public Manager reconnectionAttempts(int v) {
        _reconnectionAttempts = v;
        return this;
    }

    public final long reconnectionDelay() {
        return _reconnectionDelay;
    }

    public Manager reconnectionDelay(long v) {
        _reconnectionDelay = v;
        if (backoff != null) {
            backoff.setMin(v);
        }
        return this;
    }

    public final double randomizationFactor() {
        return _randomizationFactor;
    }

    public Manager randomizationFactor(double v) {
        _randomizationFactor = v;
        if (backoff != null) {
            backoff.setJitter(v);
        }
        return this;
    }

    public final long reconnectionDelayMax() {
        return _reconnectionDelayMax;
    }

    public Manager reconnectionDelayMax(long v) {
        _reconnectionDelayMax = v;
        if (backoff != null) {
            backoff.setMax(v);
        }
        return this;
    }

    public long timeout() {
        return _timeout;
    }

    public Manager timeout(long v) {
        _timeout = v;
        return this;
    }

    private void maybeReconnectOnOpen() {
        // Only try to reconnect if it's the first time we're connecting
        if (!reconnecting && _reconnection && backoff.getAttempts() == 0) {
            reconnect();
        }
    }

    public Manager open(){
        return open(null);
    }

    /**
     * Connects the client.
     *
     * @param fn callback.
     * @return a reference to this object.
     */
    public Manager open(final OpenCallback fn) {
        EventThread.exec(() -> {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine(String.format("readyState %s", readyState));
            }
            if (readyState == ReadyState.OPEN || readyState == ReadyState.OPENING) return;

            if (logger.isLoggable(Level.FINE)) {
                logger.fine(String.format("opening %s", uri));
            }
            engine = new Engine(uri, opts);
            final io.socket.engineio.client.Socket socket = engine;
            final Manager self = this;
            readyState = ReadyState.OPENING;
            skipReconnect = false;

            // propagate transport event.
            socket.on(Engine.EVENT_TRANSPORT, args -> self.emit(Manager.EVENT_TRANSPORT, args));

            final On.Handle openSub = On.on(socket, Engine.EVENT_OPEN, objects -> {
                self.onOpen();
                if (fn != null) fn.call(null);
            });

            On.Handle errorSub = On.on(socket, Engine.EVENT_ERROR, objects -> {
                Object data = objects.length > 0 ? objects[0] : null;
                logger.fine("connect_error");
                self.cleanup();
                self.readyState = ReadyState.CLOSED;
                self.emit(EVENT_ERROR, data);
                if (fn != null) {
                    Exception err = new SocketIOException("Connection error",
                            data instanceof Exception ? (Exception) data : null);
                    fn.call(err);
                } else {
                    // Only do this if there is no fn to handle the error
                    self.maybeReconnectOnOpen();
                }
            });

            final long timeout = _timeout;
            final Runnable onTimeout = () -> {
                logger.fine(String.format("connect attempt timed out after %d", timeout));
                openSub.destroy();
                socket.close();
                socket.emit(Engine.EVENT_ERROR, new SocketIOException("timeout"));
            };

            if (timeout == 0) {
                EventThread.exec(onTimeout);
                return;
            } else if (_timeout > 0) {
                logger.fine(String.format("connection attempt will timeout after %d", timeout));

                final Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        EventThread.exec(onTimeout);
                    }
                }, timeout);

                subs.add(timer::cancel);
            }

            subs.add(openSub);
            subs.add(errorSub);

            engine.open();
        });
        return this;
    }

    private void onOpen() {
        logger.fine("open");

        cleanup();

        readyState = ReadyState.OPEN;
        emit(EVENT_OPEN);

        final io.socket.engineio.client.Socket socket = engine;
        subs.add(On.on(socket, Engine.EVENT_DATA, objects -> {
            Object data = objects[0];
            try {
                if (data instanceof String) {
                    decoder.add((String) data);
                } else if (data instanceof byte[]) {
                    decoder.add((byte[]) data);
                }
            } catch (DecodingException e) {
                logger.fine("error while decoding the packet: " + e.getMessage());
            }
        }));
        subs.add(On.on(socket, Engine.EVENT_ERROR, objects -> onError((Exception)objects[0])));
        subs.add(On.on(socket, Engine.EVENT_CLOSE, objects -> onClose((String)objects[0])));
        decoder.onDecoded(this::onDecoded);
    }

    private void onDecoded(Packet packet) {
        emit(EVENT_PACKET, packet);
    }

    private void onError(Exception err) {
        logger.log(Level.FINE, "error", err);
        emit(EVENT_ERROR, err);
    }

    /**
     * Initializes {@link Socket} instances for each namespaces.
     *
     * @param nsp namespace.
     * @param opts options.
     * @return a socket instance for the namespace.
     */
    public Socket socket(final String nsp, Options opts) {
        synchronized (nsps) {
            Socket socket = nsps.get(nsp);
            if (socket == null) {
                socket = new Socket(this, nsp, opts);
                nsps.put(nsp, socket);
            }
            return socket;
        }
    }

    public Socket socket(String nsp) {
        return socket(nsp, null);
    }

    /*package*/ void destroy() {
        synchronized (nsps) {
            for (Socket socket : nsps.values()) {
                if (socket.isActive()) {
                    logger.fine("socket is still active, skipping close");
                    return;
                }
            }

            close();
        }
    }

    /*package*/ void packet(Packet packet) {
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(String.format("writing packet %s", packet));
        }
        final Manager self = this;

        if (!self.encoding) {
            self.encoding = true;
            encoder.encode(packet, new Parser.Encoder.Callback() {
                @Override
                public void call(Object[] encodedPackets) {
                    for (Object packet : encodedPackets) {
                        if (packet instanceof String) {
                            self.engine.write((String)packet);
                        } else if (packet instanceof byte[]) {
                            self.engine.write((byte[])packet);
                        }
                    }
                    self.encoding = false;
                    self.processPacketQueue();
                }
            });
        } else {
            self.packetBuffer.add(packet);
        }
    }

    private void processPacketQueue() {
        if (!packetBuffer.isEmpty() && !encoding) {
            Packet pack = packetBuffer.remove(0);
            packet(pack);
        }
    }

    private void cleanup() {
        logger.fine("cleanup");

        On.Handle sub;
        while ((sub = subs.poll()) != null) sub.destroy();
        decoder.onDecoded(null);

        packetBuffer.clear();
        encoding = false;

        decoder.destroy();
    }

    /*package*/ void close() {
        logger.fine("disconnect");
        skipReconnect = true;
        reconnecting = false;
        if (readyState != ReadyState.OPEN) {
            // `onclose` will not fire because
            // an open event never happened
            cleanup();
        }
        backoff.reset();
        readyState = ReadyState.CLOSED;
        if (engine != null) {
            engine.close();
        }
    }

    private void onClose(String reason) {
        logger.fine("onclose");
        cleanup();
        backoff.reset();
        readyState = ReadyState.CLOSED;
        emit(EVENT_CLOSE, reason);

        if (_reconnection && !skipReconnect) {
            reconnect();
        }
    }

    private void reconnect() {
        if (reconnecting || skipReconnect) return;

        final Manager self = this;

        if (backoff.getAttempts() >= _reconnectionAttempts) {
            logger.fine("reconnect failed");
            backoff.reset();
            emit(EVENT_RECONNECT_FAILED);
            reconnecting = false;
        } else {
            long delay = backoff.duration();
            logger.fine(String.format("will wait %dms before reconnect attempt", delay));

            reconnecting = true;
            final Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    EventThread.exec(new Runnable() {
                        @Override
                        public void run() {
                            if (self.skipReconnect) return;

                            logger.fine("attempting reconnect");
                            int attempts = self.backoff.getAttempts();
                            self.emit(EVENT_RECONNECT_ATTEMPT, attempts);

                            // check again for the case socket closed in above events
                            if (self.skipReconnect) return;

                            self.open(new OpenCallback() {
                                @Override
                                public void call(Exception err) {
                                    if (err != null) {
                                        logger.fine("reconnect attempt error");
                                        self.reconnecting = false;
                                        self.reconnect();
                                        self.emit(EVENT_RECONNECT_ERROR, err);
                                    } else {
                                        logger.fine("reconnect success");
                                        self.onReconnect();
                                    }
                                }
                            });
                        }
                    });
                }
            }, delay);

            subs.add(new On.Handle() {
                @Override
                public void destroy() {
                    timer.cancel();
                }
            });
        }
    }

    private void onReconnect() {
        int attempts = backoff.getAttempts();
        reconnecting = false;
        backoff.reset();
        emit(EVENT_RECONNECT, attempts);
    }


    public interface OpenCallback {

        void call(Exception err);
    }


    private static class Engine extends io.socket.engineio.client.Socket {

        Engine(URI uri, Options opts) {
            super(uri, opts);
        }
    }

    public static class Options extends io.socket.engineio.client.Socket.Options {

        public boolean reconnection = true;
        public int reconnectionAttempts;
        public long reconnectionDelay;
        public long reconnectionDelayMax;
        public double randomizationFactor;
        public Parser.Encoder encoder;
        public Parser.Decoder decoder;
        public Map<String, String> auth;

        /**
         * Connection timeout (ms). Set -1 to disable.
         */
        public long timeout = 20000;
    }
}
