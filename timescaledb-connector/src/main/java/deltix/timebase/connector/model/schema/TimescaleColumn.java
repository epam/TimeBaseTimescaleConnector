package deltix.timebase.connector.model.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class TimescaleColumn {

    private String name;
    private TimescaleDataType dataType;
    private List<String> relatedDescriptors;
    private boolean isArray;
    private boolean isUnique;
    private boolean isNotNull;

    public enum TimescaleDataType {
        DECIMAL, DECIMAL64, INTEGER, JSON, JSONB, LONG, CHAR, VARCHAR, DATE, TIME, DATETIME, BYTEA, BOOLEAN, UUID, SERIAL
    }
}
