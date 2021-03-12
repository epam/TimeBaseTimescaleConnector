package deltix.timebase.connector.util;

import deltix.timebase.connector.model.schema.TimescaleColumn;
import org.junit.Test;

import static deltix.timebase.connector.util.MigrationUtils.replaceDescriptorName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MigrationUtilsTest {

    @Deprecated
    @Test
    public void testReplaceDescriptorName() {
        TimescaleColumn column = TimescaleColumn.builder()
                .name("FirstClass_FieldName")
                .build();

        assertThat(replaceDescriptorName(column, "deltix.messages.SecondClass"), is("SecondClass_FieldName"));
    }
}
