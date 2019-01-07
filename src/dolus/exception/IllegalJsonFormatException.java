package dolus.exception;

/**
 * Exception when JSON input syntax is not legal
 *
 * @author m.amin rayej
 * @since 1.0
 */
public class IllegalJsonFormatException extends Exception {

    public IllegalJsonFormatException(String msg) {
        super(msg);
    }
}
