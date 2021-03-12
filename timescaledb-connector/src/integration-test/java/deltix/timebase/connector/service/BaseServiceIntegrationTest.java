package deltix.timebase.connector.service;

import deltix.timebase.connector.config.ApplicationTestConfig;
import deltix.timebase.connector.config.TimeBaseConfig;
import deltix.timebase.connector.config.TimescaleDBConfig;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@ActiveProfiles("test")
@ContextConfiguration(initializers = {
        TimescaleDBConfig.Initializer.class,
        TimeBaseConfig.Initializer.class
})
@SpringBootTest(classes = ApplicationTestConfig.class)
public class BaseServiceIntegrationTest {

    @BeforeAll
    public static void init() {
        TimescaleDBConfig.start();
        TimeBaseConfig.start();
    }
}
