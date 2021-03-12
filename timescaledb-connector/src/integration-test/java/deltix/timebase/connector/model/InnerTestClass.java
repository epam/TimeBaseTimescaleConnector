package deltix.timebase.connector.model;

import deltix.timebase.messages.SchemaElement;

public class InnerTestClass {

    private String name;
    private String value;

    public InnerTestClass(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public InnerTestClass() {
    }

    @SchemaElement
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @SchemaElement
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
