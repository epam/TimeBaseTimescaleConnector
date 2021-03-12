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
public class TimescaleSchema {

    private String name;
    private List<TimescaleColumn> columns;
    private TimescaleColumn[] primaryKey;
    private boolean isPolymorphic;
}
