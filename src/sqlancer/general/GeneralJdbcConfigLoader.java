package sqlancer.general;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import sqlancer.MainOptions;

/**
 * Utility class to load and manage JDBC configuration from properties files,
 * environment variables, and command-line options.
 *
 * Priority order (highest to lowest):
 * 1. Command-line options (--host, --port, --username, --password)
 * 2. Environment variables (SQLANCER_&lt;ENGINE&gt;_HOST, etc.)
 * 3. Properties file (dbconfigs/jdbc.properties)
 */
public final class GeneralJdbcConfigLoader {

    private static final String CONFIG_FILE = "dbconfigs/jdbc.properties";
    private static final String ENV_PREFIX = "SQLANCER_";
    private static Properties properties;
    private static boolean loaded = false;

    private GeneralJdbcConfigLoader() {
        // Utility class
    }

    /**
     * Loads the JDBC properties file. Throws an error if the file cannot be loaded.
     */
    private static synchronized void loadProperties() {
        if (loaded) {
            return;
        }
        properties = new Properties();
        File configFile = new File(CONFIG_FILE);
        if (!configFile.exists()) {
            throw new RuntimeException("JDBC configuration file not found: " + CONFIG_FILE
                    + ". Please ensure the file exists with proper database configurations.");
        }
        try (FileInputStream fis = new FileInputStream(configFile)) {
            properties.load(fis);
            loaded = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load JDBC configuration file: " + CONFIG_FILE, e);
        }
    }

    /**
     * Gets a configuration value for the specified engine and property.
     * Checks environment variables first, then the properties file.
     *
     * @param engine   The database engine name (e.g., "MYSQL", "POSTGRESQL")
     * @param property The property name (e.g., "host", "port", "url")
     * @return The configuration value, or empty string if not found
     */
    public static String getProperty(String engine, String property) {
        loadProperties();

        // 1. Check environment variable: SQLANCER_<ENGINE>_<PROPERTY>
        String envKey = ENV_PREFIX + engine.toUpperCase() + "_" + property.toUpperCase();
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }

        // 2. Check properties file: <ENGINE>.<property>
        String propKey = engine.toUpperCase() + "." + property;
        String propValue = properties.getProperty(propKey);
        if (propValue != null) {
            return propValue;
        }

        return "";
    }

    /**
     * Builds a JDBC connection string from the URL template, substituting placeholders.
     * Supports {host}, {port}, {user}, {password}, {database} placeholders.
     *
     * @param engine  The database engine name
     * @param options MainOptions for command-line overrides (can be null)
     * @return The fully constructed JDBC URL
     */
    public static String buildJdbcUrl(String engine, MainOptions options) {
        // Get URL template
        String urlTemplate = getProperty(engine, "url");
        if (urlTemplate.isEmpty()) {
            throw new RuntimeException("No JDBC URL template configured for engine: " + engine
                    + ". Please add " + engine + ".url to " + CONFIG_FILE);
        }

        // Get individual components with priority: CLI > env > properties
        String host = getHost(engine, options);
        int port = getPort(engine, options);
        String user = getUser(engine, options);
        String password = getPassword(engine, options);
        String database = getProperty(engine, "database");

        // Substitute placeholders
        return urlTemplate
                .replace("{host}", host)
                .replace("{port}", String.valueOf(port))
                .replace("{user}", user)
                .replace("{password}", password)
                .replace("{database}", database);
    }

    /**
     * Gets the host for the specified engine, considering command-line overrides.
     */
    public static String getHost(String engine, MainOptions options) {
        if (options != null && options.getHost() != null) {
            return options.getHost();
        }
        return getProperty(engine, "host");
    }

    /**
     * Gets the port for the specified engine, considering command-line overrides.
     */
    public static int getPort(String engine, MainOptions options) {
        if (options != null && options.getPort() != MainOptions.NO_SET_PORT) {
            return options.getPort();
        }
        String portStr = getProperty(engine, "port");
        if (portStr.isEmpty()) {
            throw new RuntimeException("No port configured for engine: " + engine);
        }
        try {
            return Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid port value for engine " + engine + ": " + portStr, e);
        }
    }

    /**
     * Gets the username for the specified engine, considering command-line overrides.
     */
    public static String getUser(String engine, MainOptions options) {
        if (options != null && !options.isDefaultUsername()) {
            return options.getUserName();
        }
        return getProperty(engine, "user");
    }

    /**
     * Gets the password for the specified engine, considering command-line overrides.
     */
    public static String getPassword(String engine, MainOptions options) {
        if (options != null && !options.isDefaultPassword()) {
            return options.getPassword();
        }
        return getProperty(engine, "password");
    }

    /**
     * Reloads the properties file. Useful for testing or hot-reload scenarios.
     */
    public static synchronized void reload() {
        loaded = false;
        properties = null;
        loadProperties();
    }
}
