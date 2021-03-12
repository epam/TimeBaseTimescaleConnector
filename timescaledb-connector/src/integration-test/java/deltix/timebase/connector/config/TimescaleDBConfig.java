package deltix.timebase.connector.config;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;

public class TimescaleDBConfig {

    private static final Log LOG = LogFactory.getLog(TimescaleDBConfig.class);

    private static PostgreSQLContainer timescale = new PostgreSQLContainer("timescale/timescaledb:latest-pg12")
            .withDatabaseName("test")
            .withUsername("test")
            .withPassword("test");

    public static void start() {
        timescale.start();
    }

    public static void stop() {
        timescale.stop();
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            LOG.info().append("TimescaleDB started at ").append(timescale.getJdbcUrl()).commit();
            TestPropertyValues.of("spring.datasource.url=" + timescale.getJdbcUrl(),
                    "spring.datasource.username=" + timescale.getUsername(),
                    "spring.datasource.password=" + timescale.getPassword())
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }
}
