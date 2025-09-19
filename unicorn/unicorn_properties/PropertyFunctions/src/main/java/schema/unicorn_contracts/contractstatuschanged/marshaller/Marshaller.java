package schema.unicorn_contracts.contractstatuschanged.marshaller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import schema.unicorn_contracts.contractstatuschanged.AWSEvent;
import schema.unicorn_contracts.contractstatuschanged.ContractStatusChanged;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Marshaller {

    private static final ObjectMapper MAPPER = createObjectMapper();

    public static <T> void marshal(OutputStream output, T value) throws IOException {
        MAPPER.writeValue(output, value);
    }

    public static <T> T unmarshal(InputStream input, Class<T> type) throws IOException {
        return MAPPER.readValue(input, type);
    }

    public static AWSEvent<ContractStatusChanged> unmarshalEvent(InputStream input) throws IOException {
        TypeReference<AWSEvent<ContractStatusChanged>> typeRef = new TypeReference<AWSEvent<ContractStatusChanged>>() {};
        return MAPPER.readValue(input, typeRef);
    }

    private static ObjectMapper createObjectMapper() {
        return new ObjectMapper()
                .findAndRegisterModules()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }
}