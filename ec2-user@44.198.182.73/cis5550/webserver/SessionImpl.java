package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class SessionImpl implements Session {
    String session_id;
    long time_created;
    long last_accessed;
    long max_active_interval = 300;
    Map<String, Object> kv;
    Server server;
    public SessionImpl(Server serverArg)
    {
        time_created = System.currentTimeMillis();
        last_accessed = System.currentTimeMillis();
        session_id = UUID.randomUUID().toString() + String.valueOf(time_created);
        kv = new HashMap<String, Object>();
        server = serverArg;
    }
    // Returns the session ID (the value of the SessionID cookie) that this session is associated with
    public String id()
    {
        return session_id;
    }

    // The methods below return the time this session was created, and the time time this session was
    // last accessed. The return values should be in the same format as the return value of
    // System.currentTimeMillis().
    public long creationTime() { return time_created; }
    public long lastAccessedTime()
    {
        return last_accessed;
    }
    public void setLastAccessedTime(long seconds) {last_accessed = seconds; }

    // Set the maximum time, in seconds, this session can be active without being accessed.
    public void maxActiveInterval(int seconds) { max_active_interval = seconds; }
    public long getMaxActiveInterval() { return max_active_interval; }

    // Invalidates the session. You do not need to delete the cookie on the client when this method
    // is called; it is sufficient if the session object is removed from the server.
    public void invalidate()
    {
        server.sessionMap.remove(session_id);
    }

    // The methods below look up the value for a given key, and associate a key with a new value,
    // respectively.
    public Object attribute(String name)
    {
        if(kv.containsKey(name))
            return kv.get(name);
        return null;
    }
    public void attribute(String name, Object value)
    {
        kv.put(name, value);
    }
};
