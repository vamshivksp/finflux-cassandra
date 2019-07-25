
package com.finflux.cassandra.core;

public class KeyspaceNotFoundException extends RuntimeException {

    private final String globalisationMessageCode;
    private final String defaultUserMessage;
    private final Object[] defaultUserMessageArgs;

    public KeyspaceNotFoundException(final Object... defaultUserMessageArgs) {
        this.globalisationMessageCode = "error.msg.key.space.with.identity.not.found";
        this.defaultUserMessage = "Keyspace not found exception";
        this.defaultUserMessageArgs = defaultUserMessageArgs;
    }

    public String getGlobalisationMessageCode() {
        return this.globalisationMessageCode;
    }

    public String getDefaultUserMessage() {
        return this.defaultUserMessage;
    }

    public Object[] getDefaultUserMessageArgs() {
        return this.defaultUserMessageArgs;
    }

}
