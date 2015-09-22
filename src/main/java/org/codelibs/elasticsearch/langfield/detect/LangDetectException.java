package org.codelibs.elasticsearch.langfield.detect;

/**
 * @author Nakatani Shuyo
 */
enum ErrorCode {
    NoTextError, FormatError, FileLoadError, DuplicateLangError, NeedLoadProfileError, CantDetectError, CantOpenTrainData, TrainDataFormatError, InitParamError
}

/**
 * @author Nakatani Shuyo
 * @author shinsuke
 *
 */
public class LangDetectException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final ErrorCode code;

    public LangDetectException(final ErrorCode code, final String message) {
        super(message);
        this.code = code;
    }

    public LangDetectException(final ErrorCode code, final String message,
            final Throwable t) {
        super(message, t);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }
}
