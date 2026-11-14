package ished;

/** A refusal from IshedDB: its code (invalid-argument, not-found…) and message. */
public final class IshedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String code;
    private final String reason;
    private final int status;

    public IshedException(String code, String reason, int status) {
        super(code + ": " + reason);
        this.code = code;
        this.reason = reason;
        this.status = status;
    }

    public String getCode() { return code; }
    public String getReason() { return reason; }
    public int getStatus() { return status; }
}
