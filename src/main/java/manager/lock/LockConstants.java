package manager.lock;

/**
 * This class contains all constants of lock package
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class LockConstants {

    /**
     * This class contains all supported locks
     *
     * @author m.amin.rayej
     * @since 1.0
     */
    public static class LockTypes {

        public static final int EXCLUSIVE = 1;
        public static final int UPDATE = 2;
        public static final int INTENT_EXCLUSIVE = 3;
        public static final int SHARED = 4;
        public static final int INTENT_SHARED = 5;
        public static final int NO_LOCK = 6;

    }

    /**
     * This class contains all supported lock levels
     *
     * @author m.amin.rayej
     * @since 1.0
     */
    public static class LockLevels {

        public static final int NOT_VALID_LEVEL = 0;
        public static final int DATABASE_LOCK = 1;
        public static final int TABLE_LOCK = 2;
        public static final int RECORD_LOCK = 3;

    }
}
